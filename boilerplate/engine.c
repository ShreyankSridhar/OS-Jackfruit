#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define CONTROL_SOCKET_PATH "/tmp/mini_runtime.sock"
#define MAX_CONTAINERS 64
#define MAX_ROOTFS_LEN 256
#define MAX_CMD_LEN 512
#define MAX_MSG_LEN 8192
#define MAX_LOG_PATH_LEN 256
#define STACK_SIZE (1024 * 1024)
#define LOG_CHUNK_BYTES 1024
#define LOG_QUEUE_CAPACITY 512

#define DEFAULT_SOFT_MIB 40UL
#define DEFAULT_HARD_MIB 64UL

enum request_type {
    REQ_START = 1,
    REQ_PS = 2,
    REQ_LOGS = 3,
    REQ_STOP = 4,
    REQ_STATUS = 5,
};

enum container_state {
    CONTAINER_UNUSED = 0,
    CONTAINER_STARTING = 1,
    CONTAINER_RUNNING = 2,
    CONTAINER_EXITED = 3,
    CONTAINER_STOPPED = 4,
    CONTAINER_KILLED = 5,
    CONTAINER_HARD_LIMIT_KILLED = 6,
    CONTAINER_ERROR = 7,
};

struct control_request {
    int type;
    char id[MONITOR_NAME_LEN];
    char rootfs[MAX_ROOTFS_LEN];
    char command[MAX_CMD_LEN];
    unsigned long soft_mib;
    unsigned long hard_mib;
    int nice_value;
};

struct control_response {
    int status;
    int state;
    int exit_code;
    int exit_signal;
    char message[MAX_MSG_LEN];
};

struct log_entry {
    char container_id[MONITOR_NAME_LEN];
    size_t len;
    char payload[LOG_CHUNK_BYTES];
};

struct log_queue {
    struct log_entry items[LOG_QUEUE_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    bool shutting_down;
    pthread_mutex_t mu;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
};

struct producer_ctx {
    int read_fd;
    char container_id[MONITOR_NAME_LEN];
};

struct child_config {
    char id[MONITOR_NAME_LEN];
    char rootfs[MAX_ROOTFS_LEN];
    char command[MAX_CMD_LEN];
    int log_write_fd;
    int nice_value;
};

struct container_record {
    bool active;
    char id[MONITOR_NAME_LEN];
    char rootfs[MAX_ROOTFS_LEN];
    char command[MAX_CMD_LEN];
    char log_path[MAX_LOG_PATH_LEN];
    pid_t pid;
    enum container_state state;
    time_t started_at;
    time_t finished_at;
    int exit_code;
    int exit_signal;
    bool stop_requested;
    unsigned long soft_mib;
    unsigned long hard_mib;
    int nice_value;
    pthread_t producer_thread;
    bool producer_started;
    bool producer_joined;
};

static struct container_record g_containers[MAX_CONTAINERS];
static pthread_mutex_t g_container_mu = PTHREAD_MUTEX_INITIALIZER;
static struct log_queue g_log_queue;
static volatile sig_atomic_t g_sigchld_seen = 0;
static volatile sig_atomic_t g_shutdown_requested = 0;

static volatile sig_atomic_t g_run_interrupted = 0;

static void copy_str(char *dst, size_t dst_len, const char *src)
{
    if (dst_len == 0) {
        return;
    }

    if (src == NULL) {
        dst[0] = '\0';
        return;
    }

    (void)snprintf(dst, dst_len, "%s", src);
}

static bool read_full(int fd, void *buf, size_t size)
{
    size_t off = 0;

    while (off < size) {
        ssize_t n = read(fd, (char *)buf + off, size - off);
        if (n == 0) {
            return false;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        off += (size_t)n;
    }

    return true;
}

static bool write_full(int fd, const void *buf, size_t size)
{
    size_t off = 0;

    while (off < size) {
        ssize_t n = write(fd, (const char *)buf + off, size - off);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        off += (size_t)n;
    }

    return true;
}

static bool is_running_state(enum container_state state)
{
    return state == CONTAINER_STARTING || state == CONTAINER_RUNNING;
}

static const char *state_to_string(enum container_state state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_EXITED:
        return "exited";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_HARD_LIMIT_KILLED:
        return "hard_limit_killed";
    case CONTAINER_ERROR:
        return "error";
    default:
        return "unused";
    }
}

static void format_time_or_dash(time_t ts, char *buf, size_t buf_len)
{
    if (ts <= 0) {
        copy_str(buf, buf_len, "-");
        return;
    }

    struct tm tm_info;
    if (localtime_r(&ts, &tm_info) == NULL) {
        copy_str(buf, buf_len, "-");
        return;
    }

    if (strftime(buf, buf_len, "%Y-%m-%d %H:%M:%S", &tm_info) == 0) {
        copy_str(buf, buf_len, "-");
    }
}

static int ensure_logs_dir(void)
{
    if (mkdir("logs", 0755) == 0) {
        return 0;
    }

    if (errno == EEXIST) {
        return 0;
    }

    return -1;
}

static void log_queue_init(struct log_queue *q)
{
    memset(q, 0, sizeof(*q));
    pthread_mutex_init(&q->mu, NULL);
    pthread_cond_init(&q->not_full, NULL);
    pthread_cond_init(&q->not_empty, NULL);
}

static void log_queue_shutdown(struct log_queue *q)
{
    pthread_mutex_lock(&q->mu);
    q->shutting_down = true;
    pthread_cond_broadcast(&q->not_full);
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->mu);
}

static bool log_queue_push(struct log_queue *q, const char *container_id, const char *payload, size_t len)
{
    pthread_mutex_lock(&q->mu);

    while (q->count == LOG_QUEUE_CAPACITY && !q->shutting_down) {
        pthread_cond_wait(&q->not_full, &q->mu);
    }

    if (q->shutting_down) {
        pthread_mutex_unlock(&q->mu);
        return false;
    }

    struct log_entry *entry = &q->items[q->tail];
    copy_str(entry->container_id, sizeof(entry->container_id), container_id);
    if (len > sizeof(entry->payload)) {
        len = sizeof(entry->payload);
    }
    memcpy(entry->payload, payload, len);
    entry->len = len;

    q->tail = (q->tail + 1) % LOG_QUEUE_CAPACITY;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mu);
    return true;
}

static bool log_queue_pop(struct log_queue *q, struct log_entry *out)
{
    pthread_mutex_lock(&q->mu);

    while (q->count == 0 && !q->shutting_down) {
        pthread_cond_wait(&q->not_empty, &q->mu);
    }

    if (q->count == 0 && q->shutting_down) {
        pthread_mutex_unlock(&q->mu);
        return false;
    }

    *out = q->items[q->head];
    q->head = (q->head + 1) % LOG_QUEUE_CAPACITY;
    q->count--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mu);
    return true;
}

static void *log_consumer_thread_main(void *arg)
{
    struct log_queue *q = (struct log_queue *)arg;
    struct log_entry entry;

    if (ensure_logs_dir() != 0) {
        fprintf(stderr, "warning: unable to create logs directory: %s\n", strerror(errno));
    }

    while (log_queue_pop(q, &entry)) {
        char path[MAX_LOG_PATH_LEN];
        int fd;

        (void)snprintf(path, sizeof(path), "logs/%s.log", entry.container_id);
        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            continue;
        }

        (void)write_full(fd, entry.payload, entry.len);
        close(fd);
    }

    return NULL;
}

static void *producer_thread_main(void *arg)
{
    struct producer_ctx *ctx = (struct producer_ctx *)arg;
    char buf[LOG_CHUNK_BYTES];

    while (1) {
        ssize_t n = read(ctx->read_fd, buf, sizeof(buf));
        if (n == 0) {
            break;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        if (!log_queue_push(&g_log_queue, ctx->container_id, buf, (size_t)n)) {
            break;
        }
    }

    close(ctx->read_fd);
    free(ctx);
    return NULL;
}

static int monitor_send_ioctl(unsigned long request, pid_t pid, const char *id,
    unsigned long soft_mib, unsigned long hard_mib)
{
    int fd = open("/dev/container_monitor", O_RDWR | O_CLOEXEC);
    struct monitor_request req;
    int rc;

    if (fd < 0) {
        return -1;
    }

    memset(&req, 0, sizeof(req));
    req.pid = pid;
    req.soft_limit_bytes = soft_mib * 1024UL * 1024UL;
    req.hard_limit_bytes = hard_mib * 1024UL * 1024UL;
    copy_str(req.container_id, sizeof(req.container_id), id);

    rc = ioctl(fd, request, &req);
    close(fd);
    return rc;
}

static void monitor_register_pid(pid_t pid, const char *id, unsigned long soft_mib, unsigned long hard_mib)
{
    if (monitor_send_ioctl(MONITOR_REGISTER, pid, id, soft_mib, hard_mib) != 0) {
        fprintf(stderr, "warning: monitor register failed for %s pid=%d: %s\n",
            id,
            pid,
            strerror(errno));
    }
}

static void monitor_unregister_pid(pid_t pid, const char *id)
{
    if (monitor_send_ioctl(MONITOR_UNREGISTER, pid, id, 1, 1) != 0) {
        if (errno != ENOENT) {
            fprintf(stderr, "warning: monitor unregister failed for pid=%d: %s\n",
                pid,
                strerror(errno));
        }
    }
}

static void init_response(struct control_response *resp)
{
    memset(resp, 0, sizeof(*resp));
    resp->status = 1;
    resp->state = CONTAINER_UNUSED;
    resp->exit_code = -1;
    resp->exit_signal = 0;
}

static int find_by_id_locked(const char *id)
{
    int i;

    for (i = 0; i < MAX_CONTAINERS; ++i) {
        if (g_containers[i].active && strcmp(g_containers[i].id, id) == 0) {
            return i;
        }
    }

    return -1;
}

static int find_by_pid_locked(pid_t pid)
{
    int i;

    for (i = 0; i < MAX_CONTAINERS; ++i) {
        if (g_containers[i].active && g_containers[i].pid == pid) {
            return i;
        }
    }

    return -1;
}

static int pick_slot_locked(const char *id)
{
    int i;

    for (i = 0; i < MAX_CONTAINERS; ++i) {
        if (g_containers[i].active && strcmp(g_containers[i].id, id) == 0) {
            return i;
        }
    }

    for (i = 0; i < MAX_CONTAINERS; ++i) {
        if (!g_containers[i].active) {
            return i;
        }
    }

    for (i = 0; i < MAX_CONTAINERS; ++i) {
        if (!is_running_state(g_containers[i].state)) {
            return i;
        }
    }

    return -1;
}

static bool rootfs_in_use_locked(const char *rootfs)
{
    int i;

    for (i = 0; i < MAX_CONTAINERS; ++i) {
        if (!g_containers[i].active) {
            continue;
        }
        if (is_running_state(g_containers[i].state) && strcmp(g_containers[i].rootfs, rootfs) == 0) {
            return true;
        }
    }

    return false;
}

static int count_running_locked(void)
{
    int i;
    int running = 0;

    for (i = 0; i < MAX_CONTAINERS; ++i) {
        if (g_containers[i].active && is_running_state(g_containers[i].state)) {
            running++;
        }
    }

    return running;
}

static void mark_shutdown_request(int sig)
{
    (void)sig;
    g_shutdown_requested = 1;
}

static void mark_sigchld_seen(int sig)
{
    (void)sig;
    g_sigchld_seen = 1;
}

static void mark_run_interrupted(int sig)
{
    (void)sig;
    g_run_interrupted = 1;
}

static int child_main(void *arg)
{
    struct child_config cfg = *(struct child_config *)arg;

    free(arg);

    if (setpriority(PRIO_PROCESS, 0, cfg.nice_value) != 0) {
        /* Non-fatal; continue even if nice setting fails. */
    }

    if (sethostname(cfg.id, strnlen(cfg.id, sizeof(cfg.id))) != 0) {
        dprintf(cfg.log_write_fd, "failed to sethostname: %s\n", strerror(errno));
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0) {
        if (errno != EINVAL) {
            dprintf(cfg.log_write_fd, "failed to set mount propagation: %s\n", strerror(errno));
            _exit(127);
        }
    }

    if (chroot(cfg.rootfs) != 0) {
        dprintf(cfg.log_write_fd, "chroot(%s) failed: %s\n", cfg.rootfs, strerror(errno));
        _exit(127);
    }

    if (chdir("/") != 0) {
        dprintf(cfg.log_write_fd, "chdir(/) failed: %s\n", strerror(errno));
        _exit(127);
    }

    (void)mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        if (errno != EBUSY) {
            dprintf(cfg.log_write_fd, "mount(/proc) failed: %s\n", strerror(errno));
        }
    }

    if (dup2(cfg.log_write_fd, STDOUT_FILENO) < 0 || dup2(cfg.log_write_fd, STDERR_FILENO) < 0) {
        _exit(127);
    }

    close(cfg.log_write_fd);

    execl("/bin/sh", "/bin/sh", "-c", cfg.command, (char *)NULL);
    dprintf(STDERR_FILENO, "exec failed: %s\n", strerror(errno));
    _exit(127);
}

static int start_container(const struct control_request *req, struct control_response *resp)
{
    int pipefd[2] = {-1, -1};
    void *stack = NULL;
    struct child_config *cfg = NULL;
    struct producer_ctx *pctx = NULL;
    pthread_t producer_thread;
    pid_t pid;
    int slot;

    if (req->id[0] == '\0' || req->rootfs[0] == '\0' || req->command[0] == '\0') {
        copy_str(resp->message, sizeof(resp->message), "start requires id, rootfs, and command");
        return -1;
    }

    if (req->hard_mib < req->soft_mib || req->soft_mib == 0 || req->hard_mib == 0) {
        copy_str(resp->message, sizeof(resp->message), "invalid soft/hard limits");
        return -1;
    }

    struct stat st;
    if (stat(req->rootfs, &st) != 0 || !S_ISDIR(st.st_mode)) {
        (void)snprintf(resp->message, sizeof(resp->message), "rootfs not found: %s", req->rootfs);
        return -1;
    }

    pthread_mutex_lock(&g_container_mu);
    slot = pick_slot_locked(req->id);
    if (slot < 0) {
        pthread_mutex_unlock(&g_container_mu);
        copy_str(resp->message, sizeof(resp->message), "container table full");
        return -1;
    }

    int existing = find_by_id_locked(req->id);
    if (existing >= 0 && is_running_state(g_containers[existing].state)) {
        pthread_mutex_unlock(&g_container_mu);
        copy_str(resp->message, sizeof(resp->message), "container id already running");
        return -1;
    }

    if (rootfs_in_use_locked(req->rootfs)) {
        pthread_mutex_unlock(&g_container_mu);
        copy_str(resp->message, sizeof(resp->message), "rootfs is already used by a running container");
        return -1;
    }
    pthread_mutex_unlock(&g_container_mu);

    if (ensure_logs_dir() != 0) {
        (void)snprintf(resp->message, sizeof(resp->message), "failed to create logs directory: %s", strerror(errno));
        return -1;
    }

    if (pipe(pipefd) != 0) {
        (void)snprintf(resp->message, sizeof(resp->message), "pipe failed: %s", strerror(errno));
        return -1;
    }

    cfg = calloc(1, sizeof(*cfg));
    stack = malloc(STACK_SIZE);
    if (cfg == NULL || stack == NULL) {
        copy_str(resp->message, sizeof(resp->message), "out of memory");
        goto fail;
    }

    copy_str(cfg->id, sizeof(cfg->id), req->id);
    copy_str(cfg->rootfs, sizeof(cfg->rootfs), req->rootfs);
    copy_str(cfg->command, sizeof(cfg->command), req->command);
    cfg->log_write_fd = pipefd[1];
    cfg->nice_value = req->nice_value;

    pid = clone(child_main,
        (char *)stack + STACK_SIZE,
        CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
        cfg);
    if (pid < 0) {
        (void)snprintf(resp->message, sizeof(resp->message), "clone failed: %s", strerror(errno));
        goto fail;
    }

    close(pipefd[1]);
    pipefd[1] = -1;

    pctx = calloc(1, sizeof(*pctx));
    if (pctx == NULL) {
        (void)snprintf(resp->message, sizeof(resp->message), "out of memory");
        kill(pid, SIGKILL);
        (void)waitpid(pid, NULL, 0);
        goto fail;
    }

    pctx->read_fd = pipefd[0];
    copy_str(pctx->container_id, sizeof(pctx->container_id), req->id);

    if (pthread_create(&producer_thread, NULL, producer_thread_main, pctx) != 0) {
        (void)snprintf(resp->message, sizeof(resp->message), "failed to start log producer");
        kill(pid, SIGKILL);
        (void)waitpid(pid, NULL, 0);
        free(pctx);
        goto fail;
    }

    pipefd[0] = -1;

    pthread_mutex_lock(&g_container_mu);
    struct container_record *rec = &g_containers[slot];
    memset(rec, 0, sizeof(*rec));

    rec->active = true;
    copy_str(rec->id, sizeof(rec->id), req->id);
    copy_str(rec->rootfs, sizeof(rec->rootfs), req->rootfs);
    copy_str(rec->command, sizeof(rec->command), req->command);
    (void)snprintf(rec->log_path, sizeof(rec->log_path), "logs/%s.log", req->id);
    rec->pid = pid;
    rec->state = CONTAINER_RUNNING;
    rec->started_at = time(NULL);
    rec->exit_code = -1;
    rec->exit_signal = 0;
    rec->stop_requested = false;
    rec->soft_mib = req->soft_mib;
    rec->hard_mib = req->hard_mib;
    rec->nice_value = req->nice_value;
    rec->producer_thread = producer_thread;
    rec->producer_started = true;
    rec->producer_joined = false;
    pthread_mutex_unlock(&g_container_mu);

    monitor_register_pid(pid, req->id, req->soft_mib, req->hard_mib);

    resp->status = 0;
    resp->state = CONTAINER_RUNNING;
    (void)snprintf(resp->message, sizeof(resp->message),
        "started id=%s pid=%d soft=%luMiB hard=%luMiB nice=%d",
        req->id,
        pid,
        req->soft_mib,
        req->hard_mib,
        req->nice_value);

    free(stack);
    free(cfg);
    return 0;

fail:
    if (pipefd[0] >= 0) {
        close(pipefd[0]);
    }
    if (pipefd[1] >= 0) {
        close(pipefd[1]);
    }
    free(stack);
    free(cfg);
    return -1;
}

static void reap_children(void)
{
    while (1) {
        int status = 0;
        pid_t pid = waitpid(-1, &status, WNOHANG);
        pthread_t join_thread;
        bool need_join = false;
        const char *id_for_unregister = "";

        if (pid <= 0) {
            break;
        }

        pthread_mutex_lock(&g_container_mu);
        int idx = find_by_pid_locked(pid);
        if (idx >= 0) {
            struct container_record *rec = &g_containers[idx];

            rec->finished_at = time(NULL);
            rec->state = CONTAINER_EXITED;
            rec->exit_code = -1;
            rec->exit_signal = 0;

            if (WIFEXITED(status)) {
                rec->state = CONTAINER_EXITED;
                rec->exit_code = WEXITSTATUS(status);
            } else if (WIFSIGNALED(status)) {
                int sig = WTERMSIG(status);
                rec->exit_signal = sig;
                if (rec->stop_requested) {
                    rec->state = CONTAINER_STOPPED;
                } else if (sig == SIGKILL) {
                    rec->state = CONTAINER_HARD_LIMIT_KILLED;
                } else {
                    rec->state = CONTAINER_KILLED;
                }
            } else {
                rec->state = CONTAINER_ERROR;
            }

            if (rec->producer_started && !rec->producer_joined) {
                join_thread = rec->producer_thread;
                rec->producer_joined = true;
                need_join = true;
            }

            id_for_unregister = rec->id;
        }
        pthread_mutex_unlock(&g_container_mu);

        if (need_join) {
            pthread_join(join_thread, NULL);
        }

        monitor_unregister_pid(pid, id_for_unregister);
    }
}

static void stop_all_running(int signal_value)
{
    int i;

    pthread_mutex_lock(&g_container_mu);
    for (i = 0; i < MAX_CONTAINERS; ++i) {
        if (!g_containers[i].active || !is_running_state(g_containers[i].state)) {
            continue;
        }

        g_containers[i].stop_requested = true;
        (void)kill(g_containers[i].pid, signal_value);
    }
    pthread_mutex_unlock(&g_container_mu);
}

static void join_all_producers(void)
{
    int i;

    for (i = 0; i < MAX_CONTAINERS; ++i) {
        pthread_t tid;
        bool do_join = false;

        pthread_mutex_lock(&g_container_mu);
        if (g_containers[i].active && g_containers[i].producer_started && !g_containers[i].producer_joined) {
            g_containers[i].producer_joined = true;
            tid = g_containers[i].producer_thread;
            do_join = true;
        }
        pthread_mutex_unlock(&g_container_mu);

        if (do_join) {
            pthread_join(tid, NULL);
        }
    }
}

static void build_ps_output(char *dst, size_t dst_len)
{
    size_t off = 0;
    int i;

    off += (size_t)snprintf(dst + off, dst_len - off,
        "%-16s %-8s %-17s %-9s %-9s %-7s %-19s %-19s\n",
        "id",
        "pid",
        "state",
        "soft_mib",
        "hard_mib",
        "nice",
        "started",
        "finished");

    pthread_mutex_lock(&g_container_mu);
    for (i = 0; i < MAX_CONTAINERS; ++i) {
        char started[32];
        char finished[32];
        struct container_record *rec = &g_containers[i];

        if (!rec->active) {
            continue;
        }

        format_time_or_dash(rec->started_at, started, sizeof(started));
        format_time_or_dash(rec->finished_at, finished, sizeof(finished));

        off += (size_t)snprintf(dst + off, dst_len - off,
            "%-16s %-8d %-17s %-9lu %-9lu %-7d %-19s %-19s\n",
            rec->id,
            rec->pid,
            state_to_string(rec->state),
            rec->soft_mib,
            rec->hard_mib,
            rec->nice_value,
            started,
            finished);

        if (off >= dst_len) {
            break;
        }
    }
    pthread_mutex_unlock(&g_container_mu);
}

static void read_log_tail(const char *path, char *dst, size_t dst_len)
{
    int fd;
    struct stat st;
    size_t max_read;
    off_t start;
    ssize_t n;

    if (dst_len == 0) {
        return;
    }

    fd = open(path, O_RDONLY);
    if (fd < 0) {
        (void)snprintf(dst, dst_len, "log file not found: %s", path);
        return;
    }

    if (fstat(fd, &st) != 0) {
        close(fd);
        (void)snprintf(dst, dst_len, "failed to stat log file: %s", strerror(errno));
        return;
    }

    max_read = dst_len - 1;
    start = 0;
    if ((off_t)max_read < st.st_size) {
        start = st.st_size - (off_t)max_read;
    }

    if (lseek(fd, start, SEEK_SET) < 0) {
        close(fd);
        (void)snprintf(dst, dst_len, "failed to seek log file: %s", strerror(errno));
        return;
    }

    n = read(fd, dst, max_read);
    if (n < 0) {
        close(fd);
        (void)snprintf(dst, dst_len, "failed to read log file: %s", strerror(errno));
        return;
    }

    if (start > 0 && (size_t)n + 13 < dst_len) {
        memmove(dst + 12, dst, (size_t)n);
        memcpy(dst, "[truncated]\n", 12);
        n += 12;
    }

    dst[n] = '\0';
    close(fd);
}

static void handle_start_req(const struct control_request *req, struct control_response *resp)
{
    (void)start_container(req, resp);
}

static void handle_stop_req(const struct control_request *req, struct control_response *resp)
{
    pthread_mutex_lock(&g_container_mu);
    int idx = find_by_id_locked(req->id);
    if (idx < 0) {
        pthread_mutex_unlock(&g_container_mu);
        copy_str(resp->message, sizeof(resp->message), "container not found");
        return;
    }

    struct container_record *rec = &g_containers[idx];
    if (!is_running_state(rec->state)) {
        pthread_mutex_unlock(&g_container_mu);
        copy_str(resp->message, sizeof(resp->message), "container is not running");
        return;
    }

    rec->stop_requested = true;
    pid_t pid = rec->pid;
    pthread_mutex_unlock(&g_container_mu);

    if (kill(pid, SIGTERM) != 0) {
        (void)snprintf(resp->message, sizeof(resp->message), "failed to signal pid=%d: %s", pid, strerror(errno));
        return;
    }

    resp->status = 0;
    resp->state = CONTAINER_RUNNING;
    (void)snprintf(resp->message, sizeof(resp->message), "stop signal sent to %s (pid=%d)", req->id, pid);
}

static void handle_ps_req(const struct control_request *req, struct control_response *resp)
{
    (void)req;
    build_ps_output(resp->message, sizeof(resp->message));
    resp->status = 0;
}

static void handle_logs_req(const struct control_request *req, struct control_response *resp)
{
    char path[MAX_LOG_PATH_LEN];

    if (req->id[0] == '\0') {
        copy_str(resp->message, sizeof(resp->message), "logs requires a container id");
        return;
    }

    (void)snprintf(path, sizeof(path), "logs/%s.log", req->id);
    read_log_tail(path, resp->message, sizeof(resp->message));
    resp->status = 0;
}

static void handle_status_req(const struct control_request *req, struct control_response *resp)
{
    pthread_mutex_lock(&g_container_mu);
    int idx = find_by_id_locked(req->id);
    if (idx < 0) {
        pthread_mutex_unlock(&g_container_mu);
        copy_str(resp->message, sizeof(resp->message), "container not found");
        return;
    }

    struct container_record rec = g_containers[idx];
    pthread_mutex_unlock(&g_container_mu);

    resp->status = 0;
    resp->state = rec.state;
    resp->exit_code = rec.exit_code;
    resp->exit_signal = rec.exit_signal;

    if (is_running_state(rec.state)) {
        (void)snprintf(resp->message, sizeof(resp->message),
            "id=%s pid=%d state=%s",
            rec.id,
            rec.pid,
            state_to_string(rec.state));
    } else {
        if (rec.exit_signal > 0) {
            (void)snprintf(resp->message, sizeof(resp->message),
                "id=%s state=%s signal=%d",
                rec.id,
                state_to_string(rec.state),
                rec.exit_signal);
        } else {
            (void)snprintf(resp->message, sizeof(resp->message),
                "id=%s state=%s exit_code=%d",
                rec.id,
                state_to_string(rec.state),
                rec.exit_code);
        }
    }
}

static void dispatch_request(const struct control_request *req, struct control_response *resp)
{
    init_response(resp);

    switch (req->type) {
    case REQ_START:
        handle_start_req(req, resp);
        break;
    case REQ_STOP:
        handle_stop_req(req, resp);
        break;
    case REQ_PS:
        handle_ps_req(req, resp);
        break;
    case REQ_LOGS:
        handle_logs_req(req, resp);
        break;
    case REQ_STATUS:
        handle_status_req(req, resp);
        break;
    default:
        copy_str(resp->message, sizeof(resp->message), "unknown request type");
        break;
    }
}

static void process_client(int client_fd)
{
    struct control_request req;
    struct control_response resp;

    memset(&req, 0, sizeof(req));

    if (!read_full(client_fd, &req, sizeof(req))) {
        close(client_fd);
        return;
    }

    dispatch_request(&req, &resp);
    (void)write_full(client_fd, &resp, sizeof(resp));
    close(client_fd);
}

static int setup_server_socket(void)
{
    int server_fd;
    struct sockaddr_un addr;

    server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0) {
        return -1;
    }

    unlink(CONTROL_SOCKET_PATH);

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_str(addr.sun_path, sizeof(addr.sun_path), CONTROL_SOCKET_PATH);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(server_fd);
        return -1;
    }

    (void)chmod(CONTROL_SOCKET_PATH, 0666);

    if (listen(server_fd, 64) != 0) {
        close(server_fd);
        unlink(CONTROL_SOCKET_PATH);
        return -1;
    }

    return server_fd;
}

static int supervisor_main(const char *base_rootfs)
{
    pthread_t consumer_thread;
    int server_fd;
    struct sigaction sa;

    struct stat st;
    if (stat(base_rootfs, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "base rootfs not found: %s\n", base_rootfs);
        return 1;
    }

    if (ensure_logs_dir() != 0) {
        fprintf(stderr, "failed to create logs directory: %s\n", strerror(errno));
        return 1;
    }

    log_queue_init(&g_log_queue);
    if (pthread_create(&consumer_thread, NULL, log_consumer_thread_main, &g_log_queue) != 0) {
        fprintf(stderr, "failed to start log consumer\n");
        return 1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = mark_sigchld_seen;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGCHLD, &sa, NULL) != 0) {
        fprintf(stderr, "sigaction(SIGCHLD) failed: %s\n", strerror(errno));
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = mark_shutdown_request;
    sa.sa_flags = SA_RESTART;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) != 0 || sigaction(SIGTERM, &sa, NULL) != 0) {
        fprintf(stderr, "sigaction shutdown failed: %s\n", strerror(errno));
    }

    signal(SIGPIPE, SIG_IGN);

    server_fd = setup_server_socket();
    if (server_fd < 0) {
        fprintf(stderr, "failed to create control socket: %s\n", strerror(errno));
        log_queue_shutdown(&g_log_queue);
        pthread_join(consumer_thread, NULL);
        return 1;
    }

    printf("supervisor ready (base_rootfs=%s, socket=%s)\n", base_rootfs, CONTROL_SOCKET_PATH);

    while (!g_shutdown_requested) {
        fd_set rfds;
        struct timeval tv;
        int sel;

        if (g_sigchld_seen) {
            g_sigchld_seen = 0;
            reap_children();
        }

        FD_ZERO(&rfds);
        FD_SET(server_fd, &rfds);
        tv.tv_sec = 0;
        tv.tv_usec = 250000;

        sel = select(server_fd + 1, &rfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR) {
                continue;
            }
            fprintf(stderr, "select error: %s\n", strerror(errno));
            break;
        }

        if (sel > 0 && FD_ISSET(server_fd, &rfds)) {
            int client_fd = accept(server_fd, NULL, NULL);
            if (client_fd >= 0) {
                process_client(client_fd);
            }
        }
    }

    printf("supervisor shutting down...\n");

    stop_all_running(SIGTERM);
    for (int i = 0; i < 30; ++i) {
        reap_children();
        pthread_mutex_lock(&g_container_mu);
        int running = count_running_locked();
        pthread_mutex_unlock(&g_container_mu);
        if (running == 0) {
            break;
        }
        usleep(100000);
    }

    stop_all_running(SIGKILL);
    for (int i = 0; i < 20; ++i) {
        reap_children();
        pthread_mutex_lock(&g_container_mu);
        int running = count_running_locked();
        pthread_mutex_unlock(&g_container_mu);
        if (running == 0) {
            break;
        }
        usleep(100000);
    }

    reap_children();
    join_all_producers();

    log_queue_shutdown(&g_log_queue);
    pthread_join(consumer_thread, NULL);

    close(server_fd);
    unlink(CONTROL_SOCKET_PATH);

    return 0;
}

static int connect_to_supervisor(void)
{
    int fd;
    struct sockaddr_un addr;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_str(addr.sun_path, sizeof(addr.sun_path), CONTROL_SOCKET_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static int send_request(const struct control_request *req, struct control_response *resp)
{
    int fd = connect_to_supervisor();
    if (fd < 0) {
        fprintf(stderr, "failed to connect to supervisor at %s: %s\n",
            CONTROL_SOCKET_PATH,
            strerror(errno));
        return -1;
    }

    if (!write_full(fd, req, sizeof(*req))) {
        fprintf(stderr, "failed to send request\n");
        close(fd);
        return -1;
    }

    if (!read_full(fd, resp, sizeof(*resp))) {
        fprintf(stderr, "failed to read response\n");
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

static bool parse_ulong(const char *s, unsigned long *out)
{
    char *end = NULL;
    unsigned long v;

    errno = 0;
    v = strtoul(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0') {
        return false;
    }

    *out = v;
    return true;
}

static bool parse_int(const char *s, int *out)
{
    char *end = NULL;
    long v;

    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0') {
        return false;
    }

    if (v < INT_MIN || v > INT_MAX) {
        return false;
    }

    *out = (int)v;
    return true;
}

static int parse_launch_flags(int argc, char **argv, int start_index,
    unsigned long *soft_mib, unsigned long *hard_mib, int *nice_value)
{
    int i = start_index;

    *soft_mib = DEFAULT_SOFT_MIB;
    *hard_mib = DEFAULT_HARD_MIB;
    *nice_value = 0;

    while (i < argc) {
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (i + 1 >= argc || !parse_ulong(argv[i + 1], soft_mib)) {
                fprintf(stderr, "invalid --soft-mib value\n");
                return -1;
            }
            i += 2;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (i + 1 >= argc || !parse_ulong(argv[i + 1], hard_mib)) {
                fprintf(stderr, "invalid --hard-mib value\n");
                return -1;
            }
            i += 2;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            if (i + 1 >= argc || !parse_int(argv[i + 1], nice_value)) {
                fprintf(stderr, "invalid --nice value\n");
                return -1;
            }
            if (*nice_value < -20 || *nice_value > 19) {
                fprintf(stderr, "--nice must be in [-20, 19]\n");
                return -1;
            }
            i += 2;
            continue;
        }

        fprintf(stderr, "unknown flag: %s\n", argv[i]);
        return -1;
    }

    if (*soft_mib == 0 || *hard_mib == 0 || *hard_mib < *soft_mib) {
        fprintf(stderr, "limits must satisfy 0 < soft <= hard\n");
        return -1;
    }

    return 0;
}

static void print_usage(FILE *out)
{
    fprintf(out,
        "Usage:\n"
        "  engine supervisor <base-rootfs>\n"
        "  engine start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  engine run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  engine ps\n"
        "  engine logs <id>\n"
        "  engine stop <id>\n");
}

static int run_start_or_run(int argc, char **argv, bool wait_for_exit)
{
    struct control_request req;
    struct control_request status_req;
    struct control_request stop_req;
    struct control_response resp;
    unsigned long soft_mib;
    unsigned long hard_mib;
    int nice_value;
    bool stop_sent = false;

    if (argc < 5) {
        print_usage(stderr);
        return 1;
    }

    if (parse_launch_flags(argc, argv, 5, &soft_mib, &hard_mib, &nice_value) != 0) {
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.type = REQ_START;
    copy_str(req.id, sizeof(req.id), argv[2]);
    copy_str(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_str(req.command, sizeof(req.command), argv[4]);
    req.soft_mib = soft_mib;
    req.hard_mib = hard_mib;
    req.nice_value = nice_value;

    if (send_request(&req, &resp) != 0) {
        return 1;
    }

    if (resp.status != 0) {
        fprintf(stderr, "error: %s\n", resp.message);
        return 1;
    }

    printf("%s\n", resp.message);

    if (!wait_for_exit) {
        return 0;
    }

    memset(&status_req, 0, sizeof(status_req));
    status_req.type = REQ_STATUS;
    copy_str(status_req.id, sizeof(status_req.id), req.id);

    memset(&stop_req, 0, sizeof(stop_req));
    stop_req.type = REQ_STOP;
    copy_str(stop_req.id, sizeof(stop_req.id), req.id);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = mark_run_interrupted;
    sigemptyset(&sa.sa_mask);
    (void)sigaction(SIGINT, &sa, NULL);
    (void)sigaction(SIGTERM, &sa, NULL);

    while (1) {
        if (g_run_interrupted && !stop_sent) {
            struct control_response stop_resp;
            if (send_request(&stop_req, &stop_resp) == 0) {
                fprintf(stderr, "%s\n", stop_resp.message);
            }
            stop_sent = true;
        }

        if (send_request(&status_req, &resp) != 0) {
            return 1;
        }

        if (resp.status != 0) {
            fprintf(stderr, "error: %s\n", resp.message);
            return 1;
        }

        if (!is_running_state((enum container_state)resp.state)) {
            printf("%s\n", resp.message);
            if (resp.exit_signal > 0) {
                return 128 + resp.exit_signal;
            }
            if (resp.exit_code >= 0) {
                return resp.exit_code;
            }
            return 0;
        }

        usleep(300000);
    }
}

static int run_simple_command(int type, const char *id)
{
    struct control_request req;
    struct control_response resp;

    memset(&req, 0, sizeof(req));
    req.type = type;
    if (id != NULL) {
        copy_str(req.id, sizeof(req.id), id);
    }

    if (send_request(&req, &resp) != 0) {
        return 1;
    }

    if (resp.status != 0) {
        fprintf(stderr, "error: %s\n", resp.message);
        return 1;
    }

    printf("%s\n", resp.message);
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        print_usage(stderr);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc != 3) {
            print_usage(stderr);
            return 1;
        }
        return supervisor_main(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0) {
        return run_start_or_run(argc, argv, false);
    }

    if (strcmp(argv[1], "run") == 0) {
        return run_start_or_run(argc, argv, true);
    }

    if (strcmp(argv[1], "ps") == 0) {
        if (argc != 2) {
            print_usage(stderr);
            return 1;
        }
        return run_simple_command(REQ_PS, NULL);
    }

    if (strcmp(argv[1], "logs") == 0) {
        if (argc != 3) {
            print_usage(stderr);
            return 1;
        }
        return run_simple_command(REQ_LOGS, argv[2]);
    }

    if (strcmp(argv[1], "stop") == 0) {
        if (argc != 3) {
            print_usage(stderr);
            return 1;
        }
        return run_simple_command(REQ_STOP, argv[2]);
    }

    print_usage(stderr);
    return 1;
}

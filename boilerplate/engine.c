/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
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

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 8192
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int monitor_registered;
    int producer_thread_started;
    pthread_t producer_thread;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    char container_id[CONTAINER_ID_LEN];
    int read_fd;
} producer_arg_t;

static volatile sig_atomic_t g_supervisor_stop_requested = 0;
static volatile sig_atomic_t g_supervisor_child_event = 0;
static volatile sig_atomic_t g_client_forward_stop_requested = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int write_full(int fd, const void *buf, size_t len)
{
    const char *p;
    size_t remaining;

    p = buf;
    remaining = len;

    while (remaining > 0) {
        ssize_t written = write(fd, p, remaining);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (written == 0)
            return -1;
        p += written;
        remaining -= (size_t)written;
    }

    return 0;
}

static int read_full(int fd, void *buf, size_t len)
{
    char *p;
    size_t remaining;

    p = buf;
    remaining = len;

    while (remaining > 0) {
        ssize_t nread = read(fd, p, remaining);
        if (nread < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (nread == 0)
            return -1;
        p += nread;
        remaining -= (size_t)nread;
    }

    return 0;
}

static void copy_string(char *dst, size_t dst_len, const char *src)
{
    if (dst_len == 0)
        return;

    if (!src) {
        dst[0] = '\0';
        return;
    }

    (void)snprintf(dst, dst_len, "%s", src);
}

static int ensure_log_dir(void)
{
    struct stat st;

    if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST)
        return -1;

    if (stat(LOG_DIR, &st) < 0)
        return -1;

    if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }

    return 0;
}

static int container_is_active(const container_record_t *rec)
{
    return rec->state == CONTAINER_STARTING || rec->state == CONTAINER_RUNNING;
}

static int container_is_terminal(const container_record_t *rec)
{
    return rec->state == CONTAINER_STOPPED || rec->state == CONTAINER_KILLED ||
           rec->state == CONTAINER_EXITED;
}

static container_record_t *find_container_by_id_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (strncmp(cur->id, id, sizeof(cur->id)) == 0)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (cur->host_pid == pid)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static void appendf(char *dst, size_t dst_len, size_t *offset, const char *fmt, ...)
{
    va_list ap;
    int n;

    if (*offset >= dst_len)
        return;

    va_start(ap, fmt);
    n = vsnprintf(dst + *offset, dst_len - *offset, fmt, ap);
    va_end(ap);

    if (n < 0)
        return;

    if ((size_t)n >= dst_len - *offset) {
        *offset = dst_len;
        return;
    }

    *offset += (size_t)n;
}

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int out_fd;

        memset(path, 0, sizeof(path));
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        out_fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (out_fd < 0)
            continue;

        (void)write_full(out_fd, item.data, item.length);
        close(out_fd);
    }

    return NULL;
}

static void *producer_thread(void *arg)
{
    producer_arg_t *producer = arg;
    supervisor_ctx_t *ctx = producer->ctx;

    for (;;) {
        log_item_t item;
        ssize_t n;

        memset(&item, 0, sizeof(item));
        copy_string(item.container_id, sizeof(item.container_id), producer->container_id);

        n = read(producer->read_fd, item.data, sizeof(item.data));
        if (n == 0)
            break;

        if (n < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        item.length = (size_t)n;
        if (bounded_buffer_push(&ctx->log_buffer, &item) != 0)
            break;
    }

    close(producer->read_fd);
    free(producer);
    return NULL;
}

int child_fn(void *arg)
{
    child_config_t *cfg = arg;
    int devnull_fd;

    if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
        perror("setpriority");

    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        perror("sethostname");

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0)
        perror("mount propagation private");

    if (chdir(cfg->rootfs) < 0) {
        perror("chdir rootfs");
        return 1;
    }

    if (chroot(".") < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    if (mkdir("/proc", 0555) < 0 && errno != EEXIST) {
        perror("mkdir /proc");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount proc");
        return 1;
    }

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2 log pipe");
        return 1;
    }

    close(cfg->log_write_fd);

    devnull_fd = open("/dev/null", O_RDONLY);
    if (devnull_fd >= 0) {
        (void)dup2(devnull_fd, STDIN_FILENO);
        close(devnull_fd);
    }

    execl("/bin/sh", "sh", "-c", cfg->command, (char *)NULL);
    perror("execl");
    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    copy_string(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    copy_string(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static int canonicalize_path(const char *in, char *out, size_t out_sz)
{
    char resolved[PATH_MAX];

    if (realpath(in, resolved) == NULL)
        return -1;

    if (strlen(resolved) + 1 > out_sz) {
        errno = ENAMETOOLONG;
        return -1;
    }

    copy_string(out, out_sz, resolved);
    return 0;
}

static int setup_control_socket(void)
{
    int fd;
    struct sockaddr_un addr;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    unlink(CONTROL_PATH);

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_string(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (listen(fd, 32) < 0) {
        close(fd);
        unlink(CONTROL_PATH);
        return -1;
    }

    (void)chmod(CONTROL_PATH, 0666);
    return fd;
}

static void supervisor_signal_handler(int sig)
{
    if (sig == SIGCHLD) {
        g_supervisor_child_event = 1;
        return;
    }

    if (sig == SIGINT || sig == SIGTERM)
        g_supervisor_stop_requested = 1;
}

static int install_supervisor_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGINT, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGTERM, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGCHLD, &sa, NULL) < 0)
        return -1;
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR)
        return -1;

    return 0;
}

static int spawn_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           control_response_t *resp,
                           pid_t *out_pid)
{
    char canonical_rootfs[PATH_MAX];
    char log_path[PATH_MAX];
    int pipefd[2] = {-1, -1};
    child_config_t *child_cfg = NULL;
    void *child_stack = NULL;
    char *stack_top = NULL;
    pid_t child_pid;
    container_record_t *existing;
    container_record_t *cur;
    container_record_t *record = NULL;
    producer_arg_t *producer = NULL;

    if (canonicalize_path(req->rootfs, canonical_rootfs, sizeof(canonical_rootfs)) != 0) {
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Invalid rootfs path '%s': %s",
                 req->rootfs,
                 strerror(errno));
        resp->status = 1;
        return -1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);

    existing = find_container_by_id_locked(ctx, req->container_id);
    if (existing && container_is_active(existing)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Container '%s' is already active",
                 req->container_id);
        resp->status = 1;
        return -1;
    }

    cur = ctx->containers;
    while (cur) {
        if (container_is_active(cur) && strcmp(cur->rootfs, canonical_rootfs) == 0) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(resp->message,
                     sizeof(resp->message),
                     "Rootfs '%s' is already in use by running container '%s'",
                     canonical_rootfs,
                     cur->id);
            resp->status = 1;
            return -1;
        }
        cur = cur->next;
    }

    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ensure_log_dir() != 0) {
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unable to create log directory: %s",
                 strerror(errno));
        resp->status = 1;
        return -1;
    }

    snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, req->container_id);
    {
        int log_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (log_fd < 0) {
            snprintf(resp->message,
                     sizeof(resp->message),
                     "Unable to initialize log file '%s': %s",
                     log_path,
                     strerror(errno));
            resp->status = 1;
            return -1;
        }
        close(log_fd);
    }

    if (pipe(pipefd) < 0) {
        snprintf(resp->message,
                 sizeof(resp->message),
                 "pipe() failed: %s",
                 strerror(errno));
        resp->status = 1;
        return -1;
    }

    child_cfg = calloc(1, sizeof(*child_cfg));
    child_stack = malloc(STACK_SIZE);
    if (!child_cfg || !child_stack) {
        snprintf(resp->message, sizeof(resp->message), "Out of memory while launching container");
        resp->status = 1;
        close(pipefd[0]);
        close(pipefd[1]);
        free(child_cfg);
        free(child_stack);
        return -1;
    }

    copy_string(child_cfg->id, sizeof(child_cfg->id), req->container_id);
    copy_string(child_cfg->rootfs, sizeof(child_cfg->rootfs), canonical_rootfs);
    copy_string(child_cfg->command, sizeof(child_cfg->command), req->command);
    child_cfg->nice_value = req->nice_value;
    child_cfg->log_write_fd = pipefd[1];

    stack_top = (char *)child_stack + STACK_SIZE;
    child_pid = clone(child_fn,
                      stack_top,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      child_cfg);
    if (child_pid < 0) {
        snprintf(resp->message,
                 sizeof(resp->message),
                 "clone() failed for container '%s': %s",
                 req->container_id,
                 strerror(errno));
        resp->status = 1;
        close(pipefd[0]);
        close(pipefd[1]);
        free(child_cfg);
        free(child_stack);
        return -1;
    }

    close(pipefd[1]);
    free(child_cfg);
    free(child_stack);

    record = calloc(1, sizeof(*record));
    producer = calloc(1, sizeof(*producer));
    if (!record || !producer) {
        kill(child_pid, SIGKILL);
        (void)waitpid(child_pid, NULL, 0);
        close(pipefd[0]);
        free(record);
        free(producer);
        snprintf(resp->message, sizeof(resp->message), "Out of memory after container start");
        resp->status = 1;
        return -1;
    }

    copy_string(record->id, sizeof(record->id), req->container_id);
    copy_string(record->rootfs, sizeof(record->rootfs), canonical_rootfs);
    record->host_pid = child_pid;
    record->started_at = time(NULL);
    record->state = CONTAINER_RUNNING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->exit_code = -1;
    record->exit_signal = 0;
    record->stop_requested = 0;
    record->monitor_registered = 0;
    record->producer_thread_started = 0;
    copy_string(record->log_path, sizeof(record->log_path), log_path);

    producer->ctx = ctx;
    producer->read_fd = pipefd[0];
    copy_string(producer->container_id, sizeof(producer->container_id), req->container_id);

    if (pthread_create(&record->producer_thread, NULL, producer_thread, producer) != 0) {
        kill(child_pid, SIGKILL);
        (void)waitpid(child_pid, NULL, 0);
        close(pipefd[0]);
        free(record);
        free(producer);
        snprintf(resp->message, sizeof(resp->message), "Failed to create producer thread");
        resp->status = 1;
        return -1;
    }

    record->producer_thread_started = 1;

    pthread_mutex_lock(&ctx->metadata_lock);
    if (existing && !container_is_active(existing)) {
        container_record_t **prev_next = &ctx->containers;
        while (*prev_next) {
            if (*prev_next == existing) {
                *prev_next = existing->next;
                break;
            }
            prev_next = &(*prev_next)->next;
        }
        free(existing);
    }

    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd,
                                  record->id,
                                  record->host_pid,
                                  record->soft_limit_bytes,
                                  record->hard_limit_bytes) == 0) {
            record->monitor_registered = 1;
        } else {
            fprintf(stderr,
                    "warning: monitor register failed for %s (pid=%d): %s\n",
                    record->id,
                    record->host_pid,
                    strerror(errno));
        }
    }

    *out_pid = child_pid;
    return 0;
}

static void update_record_after_exit(container_record_t *rec, int status)
{
    if (WIFEXITED(status)) {
        rec->exit_code = WEXITSTATUS(status);
        rec->exit_signal = 0;
        rec->state = CONTAINER_EXITED;
        return;
    }

    if (WIFSIGNALED(status)) {
        rec->exit_code = -1;
        rec->exit_signal = WTERMSIG(status);
        if (rec->stop_requested)
            rec->state = CONTAINER_STOPPED;
        else
            rec->state = CONTAINER_KILLED;
    }
}

static void reap_children(supervisor_ctx_t *ctx)
{
    for (;;) {
        int status = 0;
        pid_t pid;
        pthread_t producer_thread_id;
        int join_producer = 0;
        int do_unregister = 0;
        char container_id[CONTAINER_ID_LEN];

        pid = waitpid(-1, &status, WNOHANG);
        if (pid == 0)
            break;
        if (pid < 0) {
            if (errno == ECHILD)
                break;
            if (errno == EINTR)
                continue;
            break;
        }

        memset(container_id, 0, sizeof(container_id));

        pthread_mutex_lock(&ctx->metadata_lock);
        {
            container_record_t *rec = find_container_by_pid_locked(ctx, pid);
            if (rec) {
                update_record_after_exit(rec, status);
                copy_string(container_id, sizeof(container_id), rec->id);
                if (rec->producer_thread_started) {
                    producer_thread_id = rec->producer_thread;
                    rec->producer_thread_started = 0;
                    join_producer = 1;
                }
                if (rec->monitor_registered) {
                    rec->monitor_registered = 0;
                    do_unregister = 1;
                }
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (join_producer)
            (void)pthread_join(producer_thread_id, NULL);

        if (do_unregister && ctx->monitor_fd >= 0)
            (void)unregister_from_monitor(ctx->monitor_fd, container_id, pid);
    }
}

static int active_container_count(supervisor_ctx_t *ctx)
{
    int count = 0;
    container_record_t *cur;

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    while (cur) {
        if (container_is_active(cur))
            count++;
        cur = cur->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return count;
}

static void signal_all_active(supervisor_ctx_t *ctx, int sig)
{
    container_record_t *cur;

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    while (cur) {
        if (container_is_active(cur)) {
            if (sig == SIGTERM)
                cur->stop_requested = 1;
            (void)kill(cur->host_pid, sig);
        }
        cur = cur->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void wait_for_all_children(supervisor_ctx_t *ctx, int max_rounds)
{
    int i;

    for (i = 0; i < max_rounds; i++) {
        reap_children(ctx);
        if (active_container_count(ctx) == 0)
            return;
        usleep(100000);
    }
}

static void shutdown_supervisor(supervisor_ctx_t *ctx)
{
    signal_all_active(ctx, SIGTERM);
    wait_for_all_children(ctx, 30);

    if (active_container_count(ctx) > 0) {
        signal_all_active(ctx, SIGKILL);
        wait_for_all_children(ctx, 30);
    }

    bounded_buffer_begin_shutdown(&ctx->log_buffer);
    (void)pthread_join(ctx->logger_thread, NULL);

    pthread_mutex_lock(&ctx->metadata_lock);
    while (ctx->containers) {
        container_record_t *next = ctx->containers->next;
        if (ctx->containers->producer_thread_started)
            (void)pthread_join(ctx->containers->producer_thread, NULL);
        free(ctx->containers);
        ctx->containers = next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void fill_ps_response(supervisor_ctx_t *ctx, control_response_t *resp)
{
    size_t off = 0;
    container_record_t *cur;

    pthread_mutex_lock(&ctx->metadata_lock);

    appendf(resp->message,
            sizeof(resp->message),
            &off,
            "ID\tPID\tSTATE\tSOFT(MiB)\tHARD(MiB)\tEXIT\tSTARTED\n");

    cur = ctx->containers;
    while (cur) {
        char started_buf[64];
        struct tm tmv;

        memset(started_buf, 0, sizeof(started_buf));
        localtime_r(&cur->started_at, &tmv);
        strftime(started_buf, sizeof(started_buf), "%Y-%m-%d %H:%M:%S", &tmv);

        if (cur->state == CONTAINER_EXITED) {
            appendf(resp->message,
                    sizeof(resp->message),
                    &off,
                    "%s\t%d\t%s\t%lu\t%lu\tcode=%d\t%s\n",
                    cur->id,
                    cur->host_pid,
                    state_to_string(cur->state),
                    cur->soft_limit_bytes >> 20,
                    cur->hard_limit_bytes >> 20,
                    cur->exit_code,
                    started_buf);
        } else if (cur->state == CONTAINER_STOPPED || cur->state == CONTAINER_KILLED) {
            appendf(resp->message,
                    sizeof(resp->message),
                    &off,
                    "%s\t%d\t%s\t%lu\t%lu\tsig=%d\t%s\n",
                    cur->id,
                    cur->host_pid,
                    state_to_string(cur->state),
                    cur->soft_limit_bytes >> 20,
                    cur->hard_limit_bytes >> 20,
                    cur->exit_signal,
                    started_buf);
        } else {
            appendf(resp->message,
                    sizeof(resp->message),
                    &off,
                    "%s\t%d\t%s\t%lu\t%lu\t-\t%s\n",
                    cur->id,
                    cur->host_pid,
                    state_to_string(cur->state),
                    cur->soft_limit_bytes >> 20,
                    cur->hard_limit_bytes >> 20,
                    started_buf);
        }
        cur = cur->next;
    }

    pthread_mutex_unlock(&ctx->metadata_lock);

    resp->status = 0;
    if (off == 0)
        snprintf(resp->message, sizeof(resp->message), "No container metadata available");
}

static void fill_logs_response(const char *path, control_response_t *resp)
{
    int fd;
    size_t off = 0;
    int truncated = 0;

    fd = open(path, O_RDONLY);
    if (fd < 0) {
        resp->status = 1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unable to read log file '%s': %s",
                 path,
                 strerror(errno));
        return;
    }

    while (off + 1 < sizeof(resp->message)) {
        ssize_t nread = read(fd, resp->message + off, sizeof(resp->message) - off - 1);
        if (nread == 0)
            break;
        if (nread < 0) {
            if (errno == EINTR)
                continue;
            close(fd);
            resp->status = 1;
            snprintf(resp->message,
                     sizeof(resp->message),
                     "Error while reading '%s': %s",
                     path,
                     strerror(errno));
            return;
        }
        off += (size_t)nread;
        if (off + 1 == sizeof(resp->message)) {
            truncated = 1;
            break;
        }
    }

    close(fd);

    if (off == 0)
        appendf(resp->message, sizeof(resp->message), &off, "(empty log)\n");

    if (truncated)
        appendf(resp->message, sizeof(resp->message), &off, "\n[output truncated]\n");

    resp->message[off] = '\0';
    resp->status = 0;
}

static int wait_for_container_terminal(supervisor_ctx_t *ctx,
                                       const char *id,
                                       int *exit_status,
                                       control_response_t *resp)
{
    for (;;) {
        container_record_t *rec;

        if (g_supervisor_stop_requested) {
            resp->status = 1;
            snprintf(resp->message,
                     sizeof(resp->message),
                     "Supervisor stopping before run command completed");
            return -1;
        }

        reap_children(ctx);

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_id_locked(ctx, id);
        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = 1;
            snprintf(resp->message,
                     sizeof(resp->message),
                     "Container '%s' disappeared from metadata",
                     id);
            return -1;
        }

        if (container_is_terminal(rec)) {
            if (rec->state == CONTAINER_EXITED)
                *exit_status = rec->exit_code;
            else
                *exit_status = 128 + rec->exit_signal;

            snprintf(resp->message,
                     sizeof(resp->message),
                     "Container '%s' finished with status %d (%s)",
                     rec->id,
                     *exit_status,
                     state_to_string(rec->state));
            pthread_mutex_unlock(&ctx->metadata_lock);
            return 0;
        }

        pthread_mutex_unlock(&ctx->metadata_lock);
        usleep(100000);
    }
}

static void handle_stop(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *resp)
{
    container_record_t *rec;
    pid_t target_pid;
    int still_active = 0;
    int i;

    pthread_mutex_lock(&ctx->metadata_lock);
    rec = find_container_by_id_locked(ctx, req->container_id);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Unknown container '%s'",
                 req->container_id);
        return;
    }

    if (!container_is_active(rec)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 0;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Container '%s' already in terminal state (%s)",
                 req->container_id,
                 state_to_string(rec->state));
        return;
    }

    rec->stop_requested = 1;
    target_pid = rec->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (kill(target_pid, SIGTERM) < 0) {
        resp->status = 1;
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Failed to stop '%s' (pid=%d): %s",
                 req->container_id,
                 target_pid,
                 strerror(errno));
        return;
    }

    for (i = 0; i < 20; i++) {
        usleep(100000);
        reap_children(ctx);

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_id_locked(ctx, req->container_id);
        still_active = rec && container_is_active(rec);
        target_pid = rec ? rec->host_pid : -1;
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!still_active)
            break;
    }

    if (still_active && target_pid > 0)
        (void)kill(target_pid, SIGKILL);

    resp->status = 0;
    if (still_active) {
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Stop escalated to SIGKILL for '%s' (pid=%d)",
                 req->container_id,
                 target_pid);
    } else {
        snprintf(resp->message,
                 sizeof(resp->message),
                 "Stop requested for '%s' (pid=%d)",
                 req->container_id,
                 target_pid);
    }
}

static void handle_control_request(supervisor_ctx_t *ctx,
                                   const control_request_t *req,
                                   control_response_t *resp)
{
    memset(resp, 0, sizeof(*resp));

    switch (req->kind) {
    case CMD_START:
    case CMD_RUN:
    {
        pid_t child_pid;
        int run_exit_status;

        if (spawn_container(ctx, req, resp, &child_pid) != 0)
            return;

        if (req->kind == CMD_START) {
            resp->status = 0;
            snprintf(resp->message,
                     sizeof(resp->message),
                     "Started container '%s' (pid=%d)",
                     req->container_id,
                     child_pid);
            return;
        }

        if (wait_for_container_terminal(ctx,
                                        req->container_id,
                                        &run_exit_status,
                                        resp) != 0)
            return;

        resp->status = run_exit_status;
        return;
    }

    case CMD_PS:
        fill_ps_response(ctx, resp);
        return;

    case CMD_LOGS:
    {
        char path[PATH_MAX];
        container_record_t *rec;

        memset(path, 0, sizeof(path));

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_id_locked(ctx, req->container_id);
        if (rec)
            copy_string(path, sizeof(path), rec->log_path);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (path[0] == '\0')
            snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req->container_id);

        fill_logs_response(path, resp);
        return;
    }

    case CMD_STOP:
        handle_stop(ctx, req, resp);
        return;

    default:
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "Unsupported command kind: %d", req->kind);
        return;
    }
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    if (access(rootfs, R_OK | X_OK) != 0)
        fprintf(stderr,
                "warning: base-rootfs '%s' is not currently accessible: %s\n",
                rootfs,
                strerror(errno));

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (ensure_log_dir() != 0) {
        perror("mkdir logs");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr,
                "warning: unable to open /dev/container_monitor: %s\n",
                strerror(errno));
    }

    ctx.server_fd = setup_control_socket();
    if (ctx.server_fd < 0) {
        perror("setup_control_socket");
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (install_supervisor_signal_handlers() != 0) {
        perror("install_supervisor_signal_handlers");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logging_thread");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    fprintf(stderr,
            "Supervisor started. Base rootfs=%s control=%s\n",
            rootfs,
            CONTROL_PATH);

    while (!ctx.should_stop) {
        int client_fd;
        control_request_t req;
        control_response_t resp;

        if (g_supervisor_stop_requested)
            ctx.should_stop = 1;

        reap_children(&ctx);
        g_supervisor_child_event = 0;

        if (ctx.should_stop)
            break;

        client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            perror("accept");
            break;
        }

        if (g_supervisor_child_event) {
            reap_children(&ctx);
            g_supervisor_child_event = 0;
        }

        memset(&req, 0, sizeof(req));
        memset(&resp, 0, sizeof(resp));

        if (read_full(client_fd, &req, sizeof(req)) != 0) {
            resp.status = 1;
            snprintf(resp.message,
                     sizeof(resp.message),
                     "Failed to read control request: %s",
                     strerror(errno));
        } else {
            handle_control_request(&ctx, &req, &resp);
        }

        (void)write_full(client_fd, &resp, sizeof(resp));
        close(client_fd);
    }

    shutdown_supervisor(&ctx);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    close(ctx.server_fd);
    unlink(CONTROL_PATH);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

static int connect_control_socket(void)
{
    int fd;
    struct sockaddr_un addr;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_string(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static void run_client_signal_handler(int sig)
{
    (void)sig;
    g_client_forward_stop_requested = 1;
}

static void print_response_message(const control_response_t *resp)
{
    FILE *stream = stdout;
    size_t len;

    if (resp->message[0] == '\0')
        return;

    if (resp->status != 0)
        stream = stderr;

    len = strlen(resp->message);
    fputs(resp->message, stream);
    if (len == 0 || resp->message[len - 1] != '\n')
        fputc('\n', stream);
}

static void forward_stop_request_to_supervisor(const char *container_id)
{
    control_request_t stop_req;
    control_response_t stop_resp;
    int fd;

    memset(&stop_req, 0, sizeof(stop_req));
    stop_req.kind = CMD_STOP;
    copy_string(stop_req.container_id, sizeof(stop_req.container_id), container_id);

    fd = connect_control_socket();
    if (fd < 0)
        return;

    if (write_full(fd, &stop_req, sizeof(stop_req)) == 0)
        (void)read_full(fd, &stop_resp, sizeof(stop_resp));

    close(fd);
}

static int send_control_request(const control_request_t *req)
{
    int fd;
    control_response_t resp;

    fd = connect_control_socket();
    if (fd < 0) {
        fprintf(stderr,
                "Unable to connect to supervisor at %s: %s\n",
                CONTROL_PATH,
                strerror(errno));
        return 1;
    }

    if (write_full(fd, req, sizeof(*req)) != 0) {
        fprintf(stderr, "Failed to send control request: %s\n", strerror(errno));
        close(fd);
        return 1;
    }

    if (req->kind == CMD_RUN) {
        struct sigaction sa;
        struct sigaction old_int;
        struct sigaction old_term;
        size_t received = 0;
        int stop_forwarded = 0;

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = run_client_signal_handler;
        sigemptyset(&sa.sa_mask);

        g_client_forward_stop_requested = 0;
        sigaction(SIGINT, &sa, &old_int);
        sigaction(SIGTERM, &sa, &old_term);

        while (received < sizeof(resp)) {
            ssize_t n = read(fd,
                             ((char *)&resp) + received,
                             sizeof(resp) - received);
            if (n > 0) {
                received += (size_t)n;
                continue;
            }

            if (n == 0)
                break;

            if (errno == EINTR) {
                if (g_client_forward_stop_requested && !stop_forwarded) {
                    forward_stop_request_to_supervisor(req->container_id);
                    stop_forwarded = 1;
                }
                continue;
            }

            fprintf(stderr, "Failed while waiting for run response: %s\n", strerror(errno));
            sigaction(SIGINT, &old_int, NULL);
            sigaction(SIGTERM, &old_term, NULL);
            close(fd);
            return 1;
        }

        sigaction(SIGINT, &old_int, NULL);
        sigaction(SIGTERM, &old_term, NULL);

        if (received != sizeof(resp)) {
            fprintf(stderr, "Incomplete response from supervisor\n");
            close(fd);
            return 1;
        }
    } else {
        if (read_full(fd, &resp, sizeof(resp)) != 0) {
            fprintf(stderr, "Failed to read control response: %s\n", strerror(errno));
            close(fd);
            return 1;
        }
    }

    close(fd);

    print_response_message(&resp);

    if (req->kind == CMD_RUN)
        return resp.status;

    return resp.status == 0 ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);
    copy_string(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_string(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);
    copy_string(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_string(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}

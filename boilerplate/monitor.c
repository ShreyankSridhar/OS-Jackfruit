#include <linux/device.h>
#include <linux/fs.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/signal.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 15, 0)
#define MONITOR_DEL_TIMER_SYNC timer_delete_sync
#else
#define MONITOR_DEL_TIMER_SYNC del_timer_sync
#endif

struct monitored_proc {
    struct list_head node;
    pid_t pid;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    char container_id[MONITOR_NAME_LEN];
    bool soft_warned;
    bool hard_kill_sent;
};

static int g_major;
static struct class *g_class;
static struct device *g_device;
static DEFINE_MUTEX(g_list_lock);
static LIST_HEAD(g_processes);
static struct timer_list g_timer;

static unsigned int poll_interval_ms = 1000;
module_param(poll_interval_ms, uint, 0644);
MODULE_PARM_DESC(poll_interval_ms, "Memory polling interval in milliseconds");

static struct monitored_proc *find_entry_locked(pid_t pid)
{
    struct monitored_proc *entry;

    list_for_each_entry(entry, &g_processes, node) {
        if (entry->pid == pid) {
            return entry;
        }
    }

    return NULL;
}

static unsigned long task_rss_bytes(struct task_struct *task)
{
    struct mm_struct *mm;
    unsigned long rss_pages;

    mm = get_task_mm(task);
    if (!mm) {
        return 0;
    }

    rss_pages = get_mm_rss(mm);
    mmput(mm);
    return rss_pages << PAGE_SHIFT;
}

static void monitor_scan(struct timer_list *unused)
{
    struct monitored_proc *entry, *tmp;

    (void)unused;

    mutex_lock(&g_list_lock);
    list_for_each_entry_safe(entry, tmp, &g_processes, node) {
        struct task_struct *task = get_pid_task(find_vpid(entry->pid), PIDTYPE_PID);
        unsigned long rss_bytes;

        if (!task) {
            pr_info("monitor: removing stale entry pid=%d container=%s\n",
                entry->pid,
                entry->container_id);
            list_del(&entry->node);
            kfree(entry);
            continue;
        }

        rss_bytes = task_rss_bytes(task);

        if (!entry->hard_kill_sent && rss_bytes > entry->hard_limit_bytes) {
            entry->hard_kill_sent = true;
            pr_warn("monitor: HARD LIMIT container=%s pid=%d rss=%lu hard=%lu -> SIGKILL\n",
                entry->container_id,
                entry->pid,
                rss_bytes,
                entry->hard_limit_bytes);
            send_sig(SIGKILL, task, 0);
        } else if (!entry->soft_warned && rss_bytes > entry->soft_limit_bytes) {
            entry->soft_warned = true;
            pr_warn("monitor: SOFT LIMIT container=%s pid=%d rss=%lu soft=%lu\n",
                entry->container_id,
                entry->pid,
                rss_bytes,
                entry->soft_limit_bytes);
        }

        put_task_struct(task);
    }
    mutex_unlock(&g_list_lock);

    mod_timer(&g_timer, jiffies + msecs_to_jiffies(poll_interval_ms));
}

static long monitor_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    (void)file;

    if (_IOC_TYPE(cmd) != MONITOR_MAGIC) {
        return -ENOTTY;
    }

    if (copy_from_user(&req, (void __user *)arg, sizeof(req))) {
        return -EFAULT;
    }

    if (req.pid <= 0) {
        return -EINVAL;
    }

    if (req.soft_limit_bytes == 0 || req.hard_limit_bytes == 0 ||
        req.hard_limit_bytes < req.soft_limit_bytes) {
        return -EINVAL;
    }

    req.container_id[MONITOR_NAME_LEN - 1] = '\0';

    switch (cmd) {
    case MONITOR_REGISTER:
    {
        struct monitored_proc *entry;

        mutex_lock(&g_list_lock);
        entry = find_entry_locked(req.pid);
        if (entry) {
            entry->soft_limit_bytes = req.soft_limit_bytes;
            entry->hard_limit_bytes = req.hard_limit_bytes;
            strscpy(entry->container_id, req.container_id, MONITOR_NAME_LEN);
            entry->soft_warned = false;
            entry->hard_kill_sent = false;
            mutex_unlock(&g_list_lock);

            pr_info("monitor: updated container=%s pid=%d soft=%lu hard=%lu\n",
                req.container_id,
                req.pid,
                req.soft_limit_bytes,
                req.hard_limit_bytes);
            return 0;
        }

        entry = kzalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry) {
            mutex_unlock(&g_list_lock);
            return -ENOMEM;
        }

        entry->pid = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        strscpy(entry->container_id, req.container_id, MONITOR_NAME_LEN);
        INIT_LIST_HEAD(&entry->node);
        list_add_tail(&entry->node, &g_processes);
        mutex_unlock(&g_list_lock);

        pr_info("monitor: registered container=%s pid=%d soft=%lu hard=%lu\n",
            req.container_id,
            req.pid,
            req.soft_limit_bytes,
            req.hard_limit_bytes);
        return 0;
    }

    case MONITOR_UNREGISTER:
    {
        struct monitored_proc *entry;

        mutex_lock(&g_list_lock);
        entry = find_entry_locked(req.pid);
        if (!entry) {
            mutex_unlock(&g_list_lock);
            return 0;
        }

        list_del(&entry->node);
        mutex_unlock(&g_list_lock);

        pr_info("monitor: unregistered container=%s pid=%d\n",
            entry->container_id,
            entry->pid);
        kfree(entry);
        return 0;
    }

    default:
        return -ENOTTY;
    }
}

static const struct file_operations monitor_fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
#ifdef CONFIG_COMPAT
    .compat_ioctl = monitor_ioctl,
#endif
};

static int __init monitor_init(void)
{
    g_major = register_chrdev(0, DEVICE_NAME, &monitor_fops);
    if (g_major < 0) {
        pr_err("monitor: register_chrdev failed (%d)\n", g_major);
        return g_major;
    }

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    g_class = class_create(DEVICE_NAME);
#else
    g_class = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(g_class)) {
        int err = PTR_ERR(g_class);
        unregister_chrdev(g_major, DEVICE_NAME);
        return err;
    }

    g_device = device_create(g_class, NULL, MKDEV(g_major, 0), NULL, DEVICE_NAME);
    if (IS_ERR(g_device)) {
        int err = PTR_ERR(g_device);
        class_destroy(g_class);
        unregister_chrdev(g_major, DEVICE_NAME);
        return err;
    }

    timer_setup(&g_timer, monitor_scan, 0);
    mod_timer(&g_timer, jiffies + msecs_to_jiffies(poll_interval_ms));

    pr_info("monitor: loaded /dev/%s major=%d poll=%u ms\n",
        DEVICE_NAME,
        g_major,
        poll_interval_ms);
    return 0;
}

static void __exit monitor_exit(void)
{
    struct monitored_proc *entry, *tmp;

    (void)MONITOR_DEL_TIMER_SYNC(&g_timer);

    mutex_lock(&g_list_lock);
    list_for_each_entry_safe(entry, tmp, &g_processes, node) {
        list_del(&entry->node);
        kfree(entry);
    }
    mutex_unlock(&g_list_lock);

    device_destroy(g_class, MKDEV(g_major, 0));
    class_destroy(g_class);
    unregister_chrdev(g_major, DEVICE_NAME);

    pr_info("monitor: unloaded\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("OS-Jackfruit Rebuild");
MODULE_DESCRIPTION("Container memory monitor with soft/hard RSS limits");

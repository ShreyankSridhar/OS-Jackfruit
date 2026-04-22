# Project Guide (Rebuilt Repo)

This repository now contains a fresh implementation focused on these requirements:

1. Multi-container supervisor architecture in user space.
2. Explicit control IPC channel (UNIX socket) for CLI commands.
3. Bounded-buffer producer/consumer logging pipeline for stdout/stderr capture.
4. Kernel memory monitor with:
   - soft threshold warning
   - hard threshold SIGKILL policy
5. Scheduler experiments via configurable `--nice` per container.
6. Clean supervisor shutdown and process reaping.

## Suggested Evaluation Flow

1. Build user-space binaries:

```bash
make -C boilerplate ci
```

2. Build and load kernel module (VM required):

```bash
make -C boilerplate
sudo insmod boilerplate/monitor.ko
```

3. Start supervisor and run containers:

```bash
cd boilerplate
sudo ./engine supervisor ../rootfs-base
```

4. In a second terminal, run CLI commands:

```bash
sudo ./engine start alpha ../rootfs-alpha "/bin/sh -c 'echo hi; sleep 3; echo bye'"
sudo ./engine ps
sudo ./engine logs alpha
```

5. Memory-limit check:

```bash
sudo ./engine run memdemo ../rootfs-beta "exec /memory_hog 96 15" --soft-mib 32 --hard-mib 48
sudo dmesg | tail -n 100 | grep monitor:
```

6. Scheduling check:

```bash
sudo ./engine start fastcpu ../rootfs-alpha "exec /cpu_hog 12" --nice -5
sudo ./engine start slowcpu ../rootfs-beta "exec /cpu_hog 12" --nice 15
sudo ./engine ps
```

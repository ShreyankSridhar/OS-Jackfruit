# OS Jackfruit (Rebuilt)

## Team
- Shreyank Sridhar PES1UG24CS443
- Shashank arya PES1UG24CS432

This repository was rebuilt into a clean, source-focused implementation of the multi-container runtime assignment.

## What Is Included

- `boilerplate/engine.c`
  - Long-running supervisor (`engine supervisor <base-rootfs>`)
  - CLI client mode (`start`, `run`, `ps`, `logs`, `stop`)
  - Two IPC paths:
    - Control plane: UNIX socket (`/tmp/mini_runtime.sock`)
    - Logging plane: per-container pipes -> bounded producer/consumer queue -> `logs/<id>.log`
- `boilerplate/monitor.c`
  - Linux kernel module exposing `/dev/container_monitor`
  - `ioctl` register/unregister interface
  - Soft-limit warning + hard-limit SIGKILL enforcement based on RSS
- Workload helpers
  - `memory_hog.c`, `cpu_hog.c`, `io_pulse.c`
- Build and smoke CI
  - `boilerplate/Makefile`
  - `.github/workflows/submission-smoke.yml`

## Build

```bash
cd boilerplate
make ci
```

To build the kernel module as well:

```bash
make
```

## Run

From `boilerplate/`:

```bash
sudo ./engine supervisor ../rootfs-base
```

In another terminal:

```bash
sudo ./engine start alpha ../rootfs-alpha "/bin/sh -c 'echo alpha-up; sleep 5; echo alpha-done'"
sudo ./engine ps
sudo ./engine logs alpha
sudo ./engine stop alpha
```

Foreground run mode:

```bash
sudo ./engine run memdemo ../rootfs-beta "exec /memory_hog 80 12" --soft-mib 32 --hard-mib 48
```

CPU scheduling comparison:

```bash
sudo ./engine start fastcpu ../rootfs-alpha "exec /cpu_hog 14" --nice -5
sudo ./engine start slowcpu ../rootfs-beta "exec /cpu_hog 14" --nice 15
sudo ./engine ps
```

## Screenshot Evidence Commands

Run these commands and capture each screenshot immediately after the shown output.

### 1) Multi-container supervision

```bash
# terminal A
cd boilerplate
sudo ./engine supervisor ../rootfs-base

# terminal B
cd boilerplate
sudo ./engine start alpha ../rootfs-alpha "/bin/sh -c 'echo alpha-up; sleep 10; echo alpha-done'"
sudo ./engine start beta ../rootfs-beta "/bin/sh -c 'echo beta-up; sleep 10; echo beta-done'"
sudo ./engine ps
```

![01 multi-container supervision](screenshots/01_multi_container_supervision.png)

### 2) Metadata tracking (`ps`)

```bash
cd boilerplate
sudo ./engine ps
```

![02 metadata tracking](screenshots/02_metadata_tracking_ps.png)

### 3) Bounded-buffer logging pipeline

```bash
cd boilerplate
sudo ./engine logs alpha
```

![03 bounded-buffer logging](screenshots/03_bounded_buffer_logging.png)

### 4) CLI + control IPC response

```bash
cd boilerplate
sudo ./engine stop beta
```

![04 CLI IPC response](screenshots/04_cli_ipc_command_response.png)

### 5) Soft-limit warning

```bash
cd boilerplate
sudo ./engine start memdemo ../rootfs-alpha "exec /memory_hog 80 12" --soft-mib 24 --hard-mib 64
sudo dmesg | tail -n 120 | grep "SOFT LIMIT\\|monitor:"
```

![05 soft limit warning](screenshots/05_soft_limit_warning.png)

### 6) Hard-limit enforcement

```bash
cd boilerplate
sudo ./engine start memkill ../rootfs-beta "exec /memory_hog 120 15" --soft-mib 24 --hard-mib 32
sleep 2
sudo dmesg | tail -n 120 | grep "HARD LIMIT\\|monitor:"
sudo ./engine ps
```

![06 hard limit enforcement](screenshots/06_hard_limit_enforcement.png)

### 7) Scheduling experiment

```bash
cd boilerplate
sudo ./engine start fastcpu ../rootfs-alpha "exec /cpu_hog 14" --nice -5
sudo ./engine start slowcpu ../rootfs-beta "exec /cpu_hog 14" --nice 15
sleep 2
ps -o pid,ni,pcpu,etime,cmd -C cpu_hog
```

![07 scheduling experiment](screenshots/07_scheduling_experiment.png)

### 8) Clean teardown

```bash
cd boilerplate
sudo pkill -f "./engine supervisor" || true
sleep 1
ls -l /tmp/mini_runtime.sock || true
ps -ef | grep "./engine" | grep -v grep || true
```

![08 clean teardown](screenshots/08_clean_teardown.png)

## CLI Contract

```bash
engine supervisor <base-rootfs>
engine start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
engine run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
engine ps
engine logs <id>
engine stop <id>
```

Defaults:
- `--soft-mib`: `40`
- `--hard-mib`: `64`
- `--nice`: `0`

## Notes

- Containers require root privileges (`clone` namespaces + `chroot` + mounts).
- Use unique writable rootfs directories per running container (`rootfs-alpha`, `rootfs-beta`, ...).
- `logs/` and binary/module build artifacts are ignored by `.gitignore`.

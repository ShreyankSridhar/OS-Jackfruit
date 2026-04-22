obj-m += monitor.o

KDIR := /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)

USER_TARGETS := engine memory_hog cpu_hog io_pulse

all: $(USER_TARGETS) module

ci: $(USER_TARGETS)

module: monitor.ko

engine: engine.c monitor_ioctl.h
	gcc -O2 -Wall -Wextra -Werror -o engine engine.c -lpthread

memory_hog: memory_hog.c
	gcc -O2 -Wall -Wextra -Werror -o memory_hog memory_hog.c

cpu_hog: cpu_hog.c
	gcc -O2 -Wall -Wextra -Werror -o cpu_hog cpu_hog.c

io_pulse: io_pulse.c
	gcc -O2 -Wall -Wextra -Werror -o io_pulse io_pulse.c

monitor.ko: monitor.c monitor_ioctl.h
	$(MAKE) -C $(KDIR) M=$(PWD) modules

clean:
	if [ -d "$(KDIR)" ]; then $(MAKE) -C $(KDIR) M=$(PWD) clean; fi
	rm -f $(USER_TARGETS)
	rm -rf logs
	rm -f /tmp/mini_runtime.sock

.PHONY: all ci module clean

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char **argv) {
    size_t mib = 64;
    int seconds = 20;

    if (argc >= 2) {
        mib = (size_t)strtoul(argv[1], NULL, 10);
    }
    if (argc >= 3) {
        seconds = atoi(argv[2]);
    }

    size_t bytes = mib * 1024UL * 1024UL;
    char *buf = malloc(bytes);
    if (buf == NULL) {
        perror("malloc");
        return 1;
    }

    size_t page = (size_t)sysconf(_SC_PAGESIZE);
    for (size_t i = 0; i < bytes; i += page) {
        buf[i] = (char)(i & 0xff);
    }

    printf("allocated %zu MiB\n", mib);
    fflush(stdout);

    for (int i = 0; i < seconds; ++i) {
        usleep(100000);
        size_t idx = (size_t)(i * 9973) % (bytes ? bytes : 1);
        buf[idx] ^= 0x1;
    }

    printf("memory_hog done\n");
    free(buf);
    return 0;
}

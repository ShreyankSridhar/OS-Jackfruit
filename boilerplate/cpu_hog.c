#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, char **argv) {
    int seconds = 10;
    if (argc >= 2) {
        seconds = atoi(argv[1]);
    }

    time_t end = time(NULL) + seconds;
    volatile uint64_t acc = 1;

    while (time(NULL) < end) {
        for (int i = 0; i < 1000000; ++i) {
            acc = acc * 1103515245u + 12345u;
        }
    }

    printf("cpu_hog finished (%llu)\n", (unsigned long long)acc);
    return 0;
}

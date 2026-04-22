#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv) {
    int rounds = 20;
    if (argc >= 2) {
        rounds = atoi(argv[1]);
    }

    for (int i = 0; i < rounds; ++i) {
        printf("io_pulse stdout tick=%d\n", i);
        fprintf(stderr, "io_pulse stderr tick=%d\n", i);
        fflush(stdout);
        fflush(stderr);
        usleep(200000);
    }

    return 0;
}

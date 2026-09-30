#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../pubsub_common.h"

// Envia cada publicacion como un datagrama independiente.
int main(int argc, char *argv[]) {
    int socket_fd;
    long message_count = 10;
    char *end;
    struct sockaddr_in broker_address;

    if (argc < 3 || argc > 4 || strlen(argv[2]) == 0 ||
        strlen(argv[2]) >= TOPIC_MAX_LEN || strchr(argv[2], '|') != NULL ||
        strchr(argv[2], '\n') != NULL) {
        fprintf(stderr, "Usage: %s <broker-ip> <topic> [message-count]\n", argv[0]);
        return EXIT_FAILURE;
    }
    if (argc >= 4) {
        message_count = strtol(argv[3], &end, 10);
        if (*argv[3] == '\0' || *end != '\0' || message_count < 1) {
            fprintf(stderr, "message-count must be a positive integer\n");
            return EXIT_FAILURE;
        }
    }
    socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }
    memset(&broker_address, 0, sizeof(broker_address));
    broker_address.sin_family = AF_INET;
    broker_address.sin_port = htons(PORT_UDP);
    if (inet_pton(AF_INET, argv[1], &broker_address.sin_addr) != 1) {
        fprintf(stderr, "Invalid broker IPv4 address: %s\n", argv[1]);
        close(socket_fd);
        return EXIT_FAILURE;
    }

    for (long sequence = 1; sequence <= message_count; sequence++) {
        PubSubMessage message = { .type = PUBSUB_PUBLISH };
        char frame[BUFFER_SIZE];
        int length;

        memcpy(message.topic, argv[2], strlen(argv[2]) + 1);
        // El numero de secuencia permite observar reordenamiento en el subscriber.
        snprintf(message.content, sizeof(message.content), "publisher-%ld-message-%ld",
                 (long)getpid(), sequence);
        length = pubsub_encode(&message, frame, sizeof(frame));
        if (length < 0) {
            fprintf(stderr, "Message exceeds protocol limits\n");
            close(socket_fd);
            return EXIT_FAILURE;
        }
        if (sendto(socket_fd, frame, (size_t)length, 0,
                   (struct sockaddr *)&broker_address, sizeof(broker_address)) != length) {
            perror("sendto");
            close(socket_fd);
            return EXIT_FAILURE;
        }
    }

    close(socket_fd);
    return EXIT_SUCCESS;
}

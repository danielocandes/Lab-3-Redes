#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../pubsub_common.h"

// tcp a veces manda solo una parte, repetimos hasta mandar todo
static int send_all(int socket_fd, const char *buffer, size_t length) {
    size_t sent = 0;

    while (sent < length) {
        ssize_t count = send(socket_fd, buffer + sent, length - sent, 0);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            return -1;
        }
        sent += (size_t)count;
    }
    return 0;
}

// uso: publisher_tcp <ip del broker> <tema> [cuantos mensajes]
int main(int argc, char *argv[]) {
    int socket_fd;
    long message_count = 10;
    char *end;
    struct sockaddr_in broker_address;

    // revisamos los argumentos y que el tema no rompa el formato
    if (argc < 3 || argc > 4 || strlen(argv[2]) >= TOPIC_MAX_LEN ||
        strchr(argv[2], '|') != NULL || strchr(argv[2], '\n') != NULL) {
        fprintf(stderr, "Usage: %s <broker-ip> <topic> [message-count]\n", argv[0]);
        return EXIT_FAILURE;
    }
    // cuantos mensajes mandar, por defecto 10
    if (argc == 4) {
        message_count = strtol(argv[3], &end, 10);
        if (*argv[3] == '\0' || *end != '\0' || message_count < 1) {
            fprintf(stderr, "message-count must be a positive integer\n");
            return EXIT_FAILURE;
        }
    }

    // socket tcp
    socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }
    // direccion del broker: la ip que nos dieron y el puerto fijo
    memset(&broker_address, 0, sizeof(broker_address));
    broker_address.sin_family = AF_INET;
    broker_address.sin_port = htons(PORT_TCP);
    if (inet_pton(AF_INET, argv[1], &broker_address.sin_addr) != 1) {
        fprintf(stderr, "Invalid broker IPv4 address: %s\n", argv[1]);
        close(socket_fd);
        return EXIT_FAILURE;
    }
    // nos conectamos al broker, aqui pasa el handshake de tres vias
    if (connect(socket_fd, (struct sockaddr *)&broker_address, sizeof(broker_address)) != 0) {
        perror("connect");
        close(socket_fd);
        return EXIT_FAILURE;
    }

    // un mensaje por vuelta con formato P|tema|contenido
    for (long sequence = 1; sequence <= message_count; sequence++) {
        PubSubMessage message = { .type = PUBSUB_PUBLISH };
        char frame[BUFFER_SIZE];
        int length;

        memcpy(message.topic, argv[2], strlen(argv[2]) + 1);
        if (pubsub_format_sports_event(message.content, sizeof(message.content),
                                       argv[2], (long)getpid(), sequence,
                                       message_count) < 0) {
            fprintf(stderr, "Could not create sports event\n");
            close(socket_fd);
            return EXIT_FAILURE;
        }
        // lo pasamos a texto y le ponemos salto de linea para marcar donde acaba
        length = pubsub_encode(&message, frame, sizeof(frame));
        if (length < 0 || (size_t)length + 1 >= sizeof(frame)) {
            fprintf(stderr, "Message exceeds protocol limits\n");
            close(socket_fd);
            return EXIT_FAILURE;
        }
        frame[length++] = '\n';
        if (send_all(socket_fd, frame, (size_t)length) != 0) {
            perror("send");
            close(socket_fd);
            return EXIT_FAILURE;
        }
    }

    close(socket_fd);
    return EXIT_SUCCESS;
}

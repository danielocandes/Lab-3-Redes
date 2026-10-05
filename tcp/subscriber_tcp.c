/* Habilita interfaces POSIX para sockets en sistemas tipo Unix. */
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

/* Repite send hasta que la solicitud de suscripcion completa llegue al broker. */
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

/* TCP no marca mensajes: lee bytes hasta reconstruir una linea completa. */
static int receive_line(int socket_fd, char *buffer, size_t capacity) {
    size_t used = 0;

    while (used + 1 < capacity) {
        char byte;
        ssize_t count = recv(socket_fd, &byte, 1, 0);
        if (count == 0) {
            if (used == 0) {
                return 0;
            }
            break;
        }
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        buffer[used++] = byte;
        if (byte == '\n') {
            break;
        }
    }
    if (used + 1 >= capacity && buffer[used - 1] != '\n') {
        return -1;
    }
    buffer[used] = '\0';
    return 1;
}

/* Anuncia uno o mas temas y deja la conexion abierta para recibir noticias. */
int main(int argc, char *argv[]) {
    int socket_fd;
    struct sockaddr_in broker_address;
    char frame[BUFFER_SIZE];

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <broker-ip> <topic> [topic ...]\n", argv[0]);
        return EXIT_FAILURE;
    }
    /* Rechaza temas vacios, demasiado largos o que rompan el formato con |. */
    for (int index = 2; index < argc; index++) {
        if (strlen(argv[index]) == 0 || strlen(argv[index]) >= TOPIC_MAX_LEN ||
            strchr(argv[index], '|') != NULL || strchr(argv[index], '\n') != NULL) {
            fprintf(stderr, "Invalid topic: %s\n", argv[index]);
            return EXIT_FAILURE;
        }
    }

    /* SOCK_STREAM crea el socket TCP que se mantendra abierto. */
    socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }
    /* El destino es la IP recibida y el puerto TCP compartido. */
    memset(&broker_address, 0, sizeof(broker_address));
    broker_address.sin_family = AF_INET;
    broker_address.sin_port = htons(PORT_TCP);
    if (inet_pton(AF_INET, argv[1], &broker_address.sin_addr) != 1) {
        fprintf(stderr, "Invalid broker IPv4 address: %s\n", argv[1]);
        close(socket_fd);
        return EXIT_FAILURE;
    }
    /* Abre la conexion persistente hacia el broker. */
    if (connect(socket_fd, (struct sockaddr *)&broker_address, sizeof(broker_address)) != 0) {
        perror("connect");
        close(socket_fd);
        return EXIT_FAILURE;
    }

    /* Envia un mensaje S por cada tema indicado en la linea de comandos. */
    for (int index = 2; index < argc; index++) {
        PubSubMessage message = { .type = PUBSUB_SUBSCRIBE };
        int length;

        memcpy(message.topic, argv[index], strlen(argv[index]) + 1);
        length = pubsub_encode(&message, frame, sizeof(frame));
        if (length < 0 || (size_t)length + 1 >= sizeof(frame)) {
            fprintf(stderr, "Could not encode subscription\n");
            close(socket_fd);
            return EXIT_FAILURE;
        }
        /* Newline separa esta suscripcion de los mensajes siguientes del flujo. */
        frame[length++] = '\n';
        if (send_all(socket_fd, frame, (size_t)length) != 0) {
            perror("send");
            close(socket_fd);
            return EXIT_FAILURE;
        }
    }

    /* Espera indefinidamente noticias hasta que el broker cierre la conexion. */
    while (receive_line(socket_fd, frame, sizeof(frame)) == 1) {
        PubSubMessage message;
        /* Solo muestra mensajes M validos; ignora datos mal formados. */
        if (pubsub_decode(frame, &message) == 0 && message.type == PUBSUB_MESSAGE) {
            printf("[%s] %s\n", message.topic, message.content);
            fflush(stdout);
        }
    }

    close(socket_fd);
    return EXIT_SUCCESS;
}

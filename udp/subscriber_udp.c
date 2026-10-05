/* Habilita interfaces POSIX para sockets en sistemas tipo Unix. */
#define _POSIX_C_SOURCE 200809L

/* Conversion de IP y funciones de sockets UDP. */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../pubsub_common.h"

/* Abre un puerto local, registra temas con el broker y espera datagramas. */
int main(int argc, char *argv[]) {
    int socket_fd;
    struct sockaddr_in broker_address;
    struct sockaddr_in local_address;

    /* Se requiere el host del broker y al menos un tema de interes. */
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <broker-ip> <topic> [topic ...]\n", argv[0]);
        return EXIT_FAILURE;
    }
    /* Verifica que cada tema quepa y no contenga separadores del protocolo. */
    for (int index = 2; index < argc; index++) {
        if (strlen(argv[index]) == 0 || strlen(argv[index]) >= TOPIC_MAX_LEN ||
            strchr(argv[index], '|') != NULL || strchr(argv[index], '\n') != NULL) {
            fprintf(stderr, "Invalid topic: %s\n", argv[index]);
            return EXIT_FAILURE;
        }
    }

    /* Crea un socket UDP para enviar suscripciones y recibir noticias. */
    socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }
    /* Se enlaza a cualquier interfaz; puerto cero pide uno libre al sistema. */
    memset(&local_address, 0, sizeof(local_address));
    local_address.sin_family = AF_INET;
    local_address.sin_addr.s_addr = htonl(INADDR_ANY);
    local_address.sin_port = htons(0); // El sistema operativo elige un puerto libre.
    if (bind(socket_fd, (struct sockaddr *)&local_address, sizeof(local_address)) != 0) {
        perror("bind");
        close(socket_fd);
        return EXIT_FAILURE;
    }

    /* Construye el extremo remoto del broker a partir de la IP dada. */
    memset(&broker_address, 0, sizeof(broker_address));
    broker_address.sin_family = AF_INET;
    broker_address.sin_port = htons(PORT_UDP);
    if (inet_pton(AF_INET, argv[1], &broker_address.sin_addr) != 1) {
        fprintf(stderr, "Invalid broker IPv4 address: %s\n", argv[1]);
        close(socket_fd);
        return EXIT_FAILURE;
    }

    // El broker aprende la direccion de retorno a partir de estos datagramas.
    /* Envia una suscripcion por tema; el broker aprende IP y puerto de origen. */
    for (int index = 2; index < argc; index++) {
        PubSubMessage message = { .type = PUBSUB_SUBSCRIBE };
        char frame[BUFFER_SIZE];
        int length;

        memcpy(message.topic, argv[index], strlen(argv[index]) + 1);
        length = pubsub_encode(&message, frame, sizeof(frame));
        if (length < 0 ||
            sendto(socket_fd, frame, (size_t)length, 0,
                   (struct sockaddr *)&broker_address, sizeof(broker_address)) != length) {
            perror("sendto subscription");
            close(socket_fd);
            return EXIT_FAILURE;
        }
    }
    printf("Subscribed to %d topic(s); waiting for UDP messages\n", argc - 2);
    /* recvfrom espera noticias; UDP no mantiene una conexion abierta. */
    for (;;) {
        char frame[BUFFER_SIZE];
        ssize_t received = recvfrom(socket_fd, frame, sizeof(frame) - 1, 0, NULL, NULL);
        PubSubMessage message;

        if (received < 0) {
            perror("recvfrom");
            continue;
        }
        /* Convierte los bytes recibidos en una cadena C para decodificarlos. */
        frame[received] = '\0';
        /* Solo imprime mensajes M validos que el broker reenvio. */
        if (pubsub_decode(frame, &message) == 0 && message.type == PUBSUB_MESSAGE) {
            printf("[%s] %s\n", message.topic, message.content);
            fflush(stdout);
        }
    }
}

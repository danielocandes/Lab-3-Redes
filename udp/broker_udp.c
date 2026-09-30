#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "../pubsub_common.h"

typedef struct Subscription {
    char topic[TOPIC_MAX_LEN];
    struct sockaddr_in address;
    struct Subscription *next;
} Subscription;

typedef struct {
    int socket_fd;
    struct sockaddr_in destination;
    long delay_ms;
    size_t frame_length;
    char frame[BUFFER_SIZE];
} DelayedDelivery;

static Subscription *subscriptions;
static int broker_socket_fd;

// Una suscripcion UDP se identifica por IP y puerto de origen.
static int same_endpoint(const struct sockaddr_in *left,
                         const struct sockaddr_in *right) {
    return left->sin_addr.s_addr == right->sin_addr.s_addr &&
           left->sin_port == right->sin_port;
}

// Guarda cada pareja tema-direccion una sola vez.
static int register_subscription(const PubSubMessage *message,
                                 const struct sockaddr_in *address) {
    Subscription *subscription;

    for (subscription = subscriptions; subscription != NULL;
         subscription = subscription->next) {
        if (strcmp(subscription->topic, message->topic) == 0 &&
            same_endpoint(&subscription->address, address)) {
            return 0;
        }
    }

    subscription = malloc(sizeof(*subscription));
    if (subscription == NULL) {
        return -1;
    }
    memcpy(subscription->topic, message->topic, strlen(message->topic) + 1);
    subscription->address = *address;
    subscription->next = subscriptions;
    subscriptions = subscription;
    printf("Subscriber registered for %s\n", message->topic);
    fflush(stdout);
    return 0;
}

// Espera sin bloquear el loop principal y luego envia el datagrama guardado.
static void *deliver_after_delay(void *argument) {
    DelayedDelivery *delivery = argument;
    struct timespec request;
    struct timespec remaining;

    request.tv_sec = delivery->delay_ms / 1000;
    request.tv_nsec = (delivery->delay_ms % 1000) * 1000000L;
    while (nanosleep(&request, &remaining) != 0 && errno == EINTR) {
        request = remaining;
    }
    sendto(delivery->socket_fd, delivery->frame, delivery->frame_length, 0,
           (struct sockaddr *)&delivery->destination, sizeof(delivery->destination));
    free(delivery);
    return NULL;
}

// Convierte PUB a MSG y lo reenvia a las direcciones del tema.
static void deliver_message(const PubSubMessage *published, long delay_ms,
                            int delay_this_message) {
    Subscription *subscription;
    PubSubMessage delivered = *published;
    char frame[BUFFER_SIZE];
    int frame_length;

    delivered.type = PUBSUB_MESSAGE;
    frame_length = pubsub_encode(&delivered, frame, sizeof(frame));
    if (frame_length < 0) {
        return;
    }

    for (subscription = subscriptions; subscription != NULL;
         subscription = subscription->next) {
        if (strcmp(subscription->topic, delivered.topic) != 0) {
            continue;
        }
        if (delay_this_message && delay_ms > 0) {
            DelayedDelivery *delivery = malloc(sizeof(*delivery));
            pthread_t thread;

            if (delivery != NULL) {
                delivery->socket_fd = broker_socket_fd;
                delivery->destination = subscription->address;
                delivery->delay_ms = delay_ms;
                delivery->frame_length = (size_t)frame_length;
                memcpy(delivery->frame, frame, (size_t)frame_length);
                // El hilo permite que el broker reenvie los mensajes siguientes primero.
                if (pthread_create(&thread, NULL, deliver_after_delay, delivery) == 0) {
                    pthread_detach(thread);
                    continue;
                }
                free(delivery);
            }
        }
        sendto(broker_socket_fd, frame, (size_t)frame_length, 0,
               (struct sockaddr *)&subscription->address,
               sizeof(subscription->address));
    }
}

// Recibe suscripciones/publicaciones; opcionalmente retrasa solo la primera publicacion.
int main(int argc, char *argv[]) {
    long reorder_first_ms = 0;
    struct sockaddr_in address;
    int delay_next_message;

    if (argc > 2) {
        fprintf(stderr, "Usage: %s [reorder-first-message-ms]\n", argv[0]);
        return EXIT_FAILURE;
    }
    if (argc == 2) {
        char *end;
        reorder_first_ms = strtol(argv[1], &end, 10);
        if (*argv[1] == '\0' || *end != '\0' || reorder_first_ms < 0) {
            fprintf(stderr, "Delay must be a non-negative integer in milliseconds\n");
            return EXIT_FAILURE;
        }
    }
    delay_next_message = reorder_first_ms > 0;

    broker_socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (broker_socket_fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(PORT_UDP);
    if (bind(broker_socket_fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
        perror("bind");
        close(broker_socket_fd);
        return EXIT_FAILURE;
    }

    printf("UDP broker listening on port %d; first-message delay: %ld ms\n",
           PORT_UDP, reorder_first_ms);
    fflush(stdout);
    for (;;) {
        char frame[BUFFER_SIZE];
        struct sockaddr_in sender;
        socklen_t sender_length = sizeof(sender);
        ssize_t received = recvfrom(broker_socket_fd, frame, sizeof(frame) - 1, 0,
                                    (struct sockaddr *)&sender, &sender_length);
        PubSubMessage message;

        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("recvfrom");
            continue;
        }
        frame[received] = '\0';
        if (pubsub_decode(frame, &message) != 0) {
            fprintf(stderr, "Ignoring malformed datagram\n");
            continue;
        }
        if (message.type == PUBSUB_SUBSCRIBE) {
            if (message.content[0] != '\0' ||
                register_subscription(&message, &sender) != 0) {
                fprintf(stderr, "Could not register subscription\n");
            }
        } else if (message.type == PUBSUB_PUBLISH) {
            // Se retrasa una vez; las siguientes publicaciones salen de inmediato.
            int delay_this_message = delay_next_message;
            delay_next_message = 0;
            deliver_message(&message, reorder_first_ms, delay_this_message);
        }
    }
}

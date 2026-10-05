/* Habilita funciones POSIX, por ejemplo nanosleep, en sistemas tipo Unix. */
#define _POSIX_C_SOURCE 200809L

/* Bibliotecas de direcciones IPv4, sockets, hilos y pausa con nanosegundos. */
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

/* En UDP no hay conexion; el subscriber se identifica por IP y puerto. */
typedef struct Subscription {
    char topic[TOPIC_MAX_LEN];
    struct sockaddr_in address;
    struct Subscription *next;
} Subscription;

/* Datos que el hilo de demora necesita para enviar despues un datagrama. */
typedef struct {
    int socket_fd;
    struct sockaddr_in destination;
    long delay_ms;
    size_t frame_length;
    char frame[BUFFER_SIZE];
} DelayedDelivery;

/* Lista de relaciones tema-direccion que anuncia cada subscriber. */
static Subscription *subscriptions;
/* El mismo socket del broker envia respuestas a todos los subscribers. */
static int broker_socket_fd;

/* Compara el extremo de red usando IP y puerto de origen del datagrama. */
static int same_endpoint(const struct sockaddr_in *left,
                         const struct sockaddr_in *right) {
    return left->sin_addr.s_addr == right->sin_addr.s_addr &&
           left->sin_port == right->sin_port;
}

/* Guarda una suscripcion una sola vez: tema + direccion UDP del cliente. */
static int register_subscription(const PubSubMessage *message,
                                 const struct sockaddr_in *address) {
    Subscription *subscription;

    /* Busca si esta misma direccion ya pidio exactamente este tema. */
    for (subscription = subscriptions; subscription != NULL;
         subscription = subscription->next) {
        if (strcmp(subscription->topic, message->topic) == 0 &&
            same_endpoint(&subscription->address, address)) {
            return 0;
        }
    }

    /* Reserva memoria para conservar la suscripcion tras recibir el datagrama. */
    subscription = malloc(sizeof(*subscription));
    if (subscription == NULL) {
        return -1;
    }
    /* Copia tema y extremo de red y enlaza la entrada al inicio de la lista. */
    memcpy(subscription->topic, message->topic, strlen(message->topic) + 1);
    subscription->address = *address;
    subscription->next = subscriptions;
    subscriptions = subscription;
    printf("Subscriber registered for %s\n", message->topic);
    fflush(stdout);
    return 0;
}

/* Hilo auxiliar: espera el tiempo elegido y luego envia el mensaje retrasado. */
static void *deliver_after_delay(void *argument) {
    DelayedDelivery *delivery = argument;
    struct timespec request;
    struct timespec remaining;

    /* nanosleep recibe segundos y nanosegundos, no milisegundos. */
    request.tv_sec = delivery->delay_ms / 1000;
    request.tv_nsec = (delivery->delay_ms % 1000) * 1000000L;
    /* Si una senal interrumpe la espera, nanosleep informa cuanto faltaba. */
    while (nanosleep(&request, &remaining) != 0 && errno == EINTR) {
        request = remaining;
    }
    /* Envia el datagrama guardado despues del retraso; luego libera su copia. */
    sendto(delivery->socket_fd, delivery->frame, delivery->frame_length, 0,
           (struct sockaddr *)&delivery->destination, sizeof(delivery->destination));
    free(delivery);
    return NULL;
}

/* Convierte P en M y manda la noticia a cada direccion suscrita al tema. */
static void deliver_message(const PubSubMessage *published, long delay_ms,
                            int delay_this_message) {
    Subscription *subscription;
    PubSubMessage delivered = *published;
    char frame[BUFFER_SIZE];
    int frame_length;

    /* El publisher pide P; el mensaje que se entrega al subscriber es M. */
    delivered.type = PUBSUB_MESSAGE;
    frame_length = pubsub_encode(&delivered, frame, sizeof(frame));
    if (frame_length < 0) {
        return;
    }

    /* Revisa todos los subscribers y solo elige los del tema de la noticia. */
    for (subscription = subscriptions; subscription != NULL;
         subscription = subscription->next) {
        if (strcmp(subscription->topic, delivered.topic) != 0) {
            continue;
        }
        /* Solo la primera noticia se demora; las siguientes salen ya. */
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
                /* Hilo separado: el loop UDP puede seguir recibiendo publicaciones. */
                if (pthread_create(&thread, NULL, deliver_after_delay, delivery) == 0) {
                    pthread_detach(thread);
                    continue;
                }
                free(delivery);
            }
        }
        /* En el caso normal el broker reenvia directamente el datagrama. */
        sendto(broker_socket_fd, frame, (size_t)frame_length, 0,
               (struct sockaddr *)&subscription->address,
               sizeof(subscription->address));
    }
}

/* El broker UDP recibe datagramas y opcionalmente desordena el primero. */
int main(int argc, char *argv[]) {
    long reorder_first_ms = 0;
    struct sockaddr_in address;
    int delay_next_message;

    /* El argumento opcional sirve para observar reordenamiento en la demo. */
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
    /* Un retraso cero desactiva por completo la prueba de reordenamiento. */
    delay_next_message = reorder_first_ms > 0;

    /* SOCK_DGRAM crea un socket UDP: no hay connect ni sesiones persistentes. */
    broker_socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (broker_socket_fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }
    /* El broker escucha en todas las interfaces locales y en el puerto UDP. */
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
    /* Cada vuelta procesa un datagrama completo de un cliente. */
    for (;;) {
        char frame[BUFFER_SIZE];
        struct sockaddr_in sender;
        socklen_t sender_length = sizeof(sender);
        /* recvfrom devuelve bytes y tambien la IP/puerto del remitente. */
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
        /* Agrega terminador para poder validar el datagrama como texto C. */
        frame[received] = '\0';
        if (pubsub_decode(frame, &message) != 0) {
            fprintf(stderr, "Ignoring malformed datagram\n");
            continue;
        }
        /* S registra quien escucha; P se reenvia a quienes escuchan ese tema. */
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

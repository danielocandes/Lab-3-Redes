#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../pubsub_common.h"

typedef struct Subscription {
    int socket_fd;
    char topic[TOPIC_MAX_LEN];
    struct Subscription *next;
} Subscription;

typedef struct {
    int socket_fd;
} ClientConnection;

static Subscription *subscriptions;
static pthread_mutex_t subscribers_mutex = PTHREAD_MUTEX_INITIALIZER;

// TCP puede aceptar envios parciales; repetir hasta completar el mensaje.
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

// TCP es un flujo, asi que newline marca donde termina cada mensaje.
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

// Agrega una pareja socket-tema; el mutex protege la lista compartida.
static int add_subscription(int socket_fd, const char *name) {
    Subscription *subscription;

    pthread_mutex_lock(&subscribers_mutex);
    for (subscription = subscriptions; subscription != NULL;
         subscription = subscription->next) {
        if (subscription->socket_fd == socket_fd &&
            strcmp(subscription->topic, name) == 0) {
            pthread_mutex_unlock(&subscribers_mutex);
            return 0;
        }
    }
    subscription = malloc(sizeof(*subscription));
    if (subscription == NULL) {
        pthread_mutex_unlock(&subscribers_mutex);
        return -1;
    }
    subscription->socket_fd = socket_fd;
    memcpy(subscription->topic, name, strlen(name) + 1);
    subscription->next = subscriptions;
    subscriptions = subscription;
    pthread_mutex_unlock(&subscribers_mutex);
    return 0;
}

// Cambia PUB por MSG y lo envia a cada socket suscrito a ese tema.
static void publish_message(const PubSubMessage *published) {
    PubSubMessage delivered = *published;
    Subscription *subscription;
    char frame[BUFFER_SIZE];
    int length;

    delivered.type = PUBSUB_MESSAGE;
    length = pubsub_encode(&delivered, frame, sizeof(frame));
    if (length < 0 || (size_t)length + 1 >= sizeof(frame)) {
        return;
    }
    frame[length++] = '\n';

    // El mismo mutex evita escrituras simultaneas en una conexion TCP.
    pthread_mutex_lock(&subscribers_mutex);
    for (subscription = subscriptions; subscription != NULL;
         subscription = subscription->next) {
        if (strcmp(subscription->topic, delivered.topic) == 0 &&
            send_all(subscription->socket_fd, frame, (size_t)length) != 0) {
            shutdown(subscription->socket_fd, SHUT_RDWR);
        }
    }
    pthread_mutex_unlock(&subscribers_mutex);
}

// Quita todas las suscripciones de un socket cuando el cliente se desconecta.
static void remove_subscriber(int socket_fd) {
    Subscription **current = &subscriptions;

    pthread_mutex_lock(&subscribers_mutex);
    while (*current != NULL) {
        if ((*current)->socket_fd == socket_fd) {
            Subscription *removed = *current;
            *current = removed->next;
            free(removed);
        } else {
            current = &(*current)->next;
        }
    }
    pthread_mutex_unlock(&subscribers_mutex);
}

// Lee publicaciones consecutivas de una conexion publisher.
static void serve_publisher(int socket_fd, const PubSubMessage *first_message) {
    PubSubMessage message = *first_message;
    char frame[BUFFER_SIZE];

    for (;;) {
        if (message.type == PUBSUB_PUBLISH) {
            publish_message(&message);
        }
        if (receive_line(socket_fd, frame, sizeof(frame)) != 1 ||
            pubsub_decode(frame, &message) != 0 ||
            message.type != PUBSUB_PUBLISH) {
            break;
        }
    }
}

// Registra los temas y mantiene la conexion abierta para recibir mas suscripciones.
static void serve_subscriber(int socket_fd, const PubSubMessage *first_message) {
    PubSubMessage message;
    char frame[BUFFER_SIZE];

    if (add_subscription(socket_fd, first_message->topic) != 0) {
        return;
    }
    printf("Subscriber registered for %s\n", first_message->topic);
    fflush(stdout);

    while (receive_line(socket_fd, frame, sizeof(frame)) == 1) {
        if (pubsub_decode(frame, &message) != 0 || message.type != PUBSUB_SUBSCRIBE) {
            break;
        }
        if (add_subscription(socket_fd, message.topic) != 0) {
            break;
        }
    }
    remove_subscriber(socket_fd);
}

// Cada hilo clasifica su primera linea y atiende esa conexion hasta cerrarla.
static void *client_worker(void *argument) {
    ClientConnection *connection = argument;
    int socket_fd = connection->socket_fd;
    PubSubMessage first_message;
    char frame[BUFFER_SIZE];

    free(connection);
    if (receive_line(socket_fd, frame, sizeof(frame)) == 1 &&
        pubsub_decode(frame, &first_message) == 0) {
        if (first_message.type == PUBSUB_PUBLISH) {
            serve_publisher(socket_fd, &first_message);
        } else if (first_message.type == PUBSUB_SUBSCRIBE) {
            serve_subscriber(socket_fd, &first_message);
        }
    }
    close(socket_fd);
    return NULL;
}

// Abre el puerto TCP y crea un hilo independiente por cada conexion aceptada.
int main(void) {
    int server_fd;
    int reuse_address = 1;
    struct sockaddr_in address;

    signal(SIGPIPE, SIG_IGN); // Un cliente desconectado no debe cerrar todo el broker.
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse_address,
                   sizeof(reuse_address)) != 0) {
        perror("setsockopt");
        close(server_fd);
        return EXIT_FAILURE;
    }

    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(PORT_TCP);
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) != 0 ||
        listen(server_fd, 16) != 0) {
        perror("bind/listen");
        close(server_fd);
        return EXIT_FAILURE;
    }

    printf("TCP broker listening on port %d\n", PORT_TCP);
    fflush(stdout);
    for (;;) {
        int client_fd = accept(server_fd, NULL, NULL);
        ClientConnection *connection;
        pthread_t thread;

        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("accept");
            continue;
        }
        connection = malloc(sizeof(*connection));
        if (connection == NULL) {
            close(client_fd);
            continue;
        }
        connection->socket_fd = client_fd;
        if (pthread_create(&thread, NULL, client_worker, connection) != 0) {
            close(client_fd);
            free(connection);
            continue;
        }
        pthread_detach(thread); // El hilo se libera solo cuando termina.
    }
}

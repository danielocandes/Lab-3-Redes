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
// maximo de temas por suscriptor
#define MAX_SUBSCRIBER_TOPICS 16
// si un suscriptor acumula tantos mensajes sin leer lo desconectamos
#define MAX_PENDING 1000
// un mensaje que espera su turno para enviarse
typedef struct PendingFrame {
    char frame[BUFFER_SIZE];
    size_t length;
    struct PendingFrame *next;
} PendingFrame;
// un suscriptor es una conexion con su cola de mensajes y su propio hilo que envia
typedef struct Subscriber {
    int socket_fd;
    char topics[MAX_SUBSCRIBER_TOPICS][TOPIC_MAX_LEN];
    size_t topic_count;
    PendingFrame *first;
    PendingFrame *last;
    size_t pending;
    pthread_mutex_t mutex;
    pthread_cond_t ready;
    int closed;
    pthread_t sender;
    struct Subscriber *next;
} Subscriber;
// lo que le pasamos al hilo cuando llega un cliente
typedef struct { int socket_fd; } ClientConnection;
// lista de todos los suscriptores conectados
static Subscriber *subscribers;
// candado de la lista, solo un hilo la toca a la vez
static pthread_mutex_t subscribers_mutex = PTHREAD_MUTEX_INITIALIZER;
static void *subscriber_sender(void *argument);
// crea el suscriptor y arranca su hilo de envio
static Subscriber *subscriber_new(int socket_fd, const char *topic) {
    Subscriber *subscriber = calloc(1, sizeof(*subscriber));
    if (subscriber == NULL) return NULL;
    subscriber->socket_fd = socket_fd;
    memcpy(subscriber->topics[0], topic, strlen(topic) + 1);
    subscriber->topic_count = 1;
    if (pthread_mutex_init(&subscriber->mutex, NULL) != 0) {
        free(subscriber);
        return NULL;
    }
    if (pthread_cond_init(&subscriber->ready, NULL) != 0) {
        pthread_mutex_destroy(&subscriber->mutex);
        free(subscriber);
        return NULL;
    }
    if (pthread_create(&subscriber->sender, NULL, subscriber_sender,
                       subscriber) != 0) {
        pthread_cond_destroy(&subscriber->ready);
        pthread_mutex_destroy(&subscriber->mutex);
        free(subscriber);
        return NULL;
    }
    return subscriber;
}
// el publisher deja el mensaje en la cola del suscriptor y sigue sin esperar
static int subscriber_push(Subscriber *subscriber, const char *frame,
                           size_t length) {
    PendingFrame *pending = malloc(sizeof(*pending));
    if (pending == NULL) return -1;
    memcpy(pending->frame, frame, length);
    pending->length = length;
    pending->next = NULL;
    pthread_mutex_lock(&subscriber->mutex);
    if (subscriber->closed || subscriber->pending >= MAX_PENDING) {
        pthread_mutex_unlock(&subscriber->mutex);
        free(pending);
        return -1;
    }
    if (subscriber->last == NULL) {
        subscriber->first = pending;
    } else {
        subscriber->last->next = pending;
    }
    subscriber->last = pending;
    subscriber->pending++;
    pthread_cond_signal(&subscriber->ready);
    pthread_mutex_unlock(&subscriber->mutex);
    return 0;
}
// el hilo de envio saca el siguiente mensaje, si no hay se duerme hasta que llegue uno
static int subscriber_pop(Subscriber *subscriber, char *frame, size_t *length) {
    PendingFrame *pending;
    pthread_mutex_lock(&subscriber->mutex);
    while (subscriber->first == NULL && !subscriber->closed) {
        pthread_cond_wait(&subscriber->ready, &subscriber->mutex);
    }
    pending = subscriber->first;
    if (pending != NULL) {
        subscriber->first = pending->next;
        subscriber->pending--;
        if (subscriber->first == NULL) {
            subscriber->last = NULL;
        }
    }
    pthread_mutex_unlock(&subscriber->mutex);
    if (pending == NULL) return 0;
    memcpy(frame, pending->frame, pending->length);
    *length = pending->length;
    free(pending);
    return 1;
}
// avisa al hilo de envio que ya se acaba
static void subscriber_close(Subscriber *subscriber) {
    pthread_mutex_lock(&subscriber->mutex);
    subscriber->closed = 1;
    pthread_cond_signal(&subscriber->ready);
    pthread_mutex_unlock(&subscriber->mutex);
}
// libera la cola y el suscriptor
static void subscriber_free(Subscriber *subscriber) {
    PendingFrame *pending = subscriber->first;
    while (pending != NULL) {
        PendingFrame *next = pending->next;
        free(pending);
        pending = next;
    }
    pthread_cond_destroy(&subscriber->ready);
    pthread_mutex_destroy(&subscriber->mutex);
    free(subscriber);
}
// tcp puede mandar solo una parte, asi que repetimos hasta mandar todo
static int send_all(int socket_fd, const char *buffer, size_t length) {
    size_t sent = 0;
    while (sent < length) {
        ssize_t count = send(socket_fd, buffer + sent, length - sent, 0);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) return -1;
        sent += (size_t)count;
    }
    return 0;
}
// este hilo es el unico que escribe en el socket de este suscriptor
static void *subscriber_sender(void *argument) {
    Subscriber *subscriber = argument;
    char frame[BUFFER_SIZE];
    size_t length;
    while (subscriber_pop(subscriber, frame, &length)) {
        if (send_all(subscriber->socket_fd, frame, length) != 0) {
            shutdown(subscriber->socket_fd, SHUT_RDWR);
            break;
        }
    }
    return NULL;
}
// tcp no marca donde acaba cada mensaje, leemos byte por byte hasta el salto de linea
static int receive_line(int socket_fd, char *buffer, size_t capacity) {
    size_t used = 0;
    while (used + 1 < capacity) {
        char byte;
        ssize_t count = recv(socket_fd, &byte, 1, 0);
        if (count == 0) {
            if (used == 0) return 0;
            break;
        }
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        buffer[used++] = byte;
        if (byte == '\n') break;
    }
    if (used + 1 >= capacity && buffer[used - 1] != '\n') return -1;
    buffer[used] = '\0';
    return 1;
}
// mira si el suscriptor pidio ese tema
static int subscriber_has_topic(const Subscriber *subscriber, const char *name) {
    size_t index;
    for (index = 0; index < subscriber->topic_count; index++) {
        if (strcmp(subscriber->topics[index], name) == 0) {
            return 1;
        }
    }
    return 0;
}
// agrega un tema nuevo al suscriptor
static int subscriber_add_topic(Subscriber *subscriber, const char *name) {
    int result = 0;
    pthread_mutex_lock(&subscribers_mutex);
    if (!subscriber_has_topic(subscriber, name)) {
        if (subscriber->topic_count == MAX_SUBSCRIBER_TOPICS) {
            result = -1;
        } else {
            memcpy(subscriber->topics[subscriber->topic_count], name,
                   strlen(name) + 1);
            subscriber->topic_count++;
        }
    }
    pthread_mutex_unlock(&subscribers_mutex);
    return result;
}
// cambia la P por M y deja el mensaje en la cola de cada suscriptor de ese tema
static void publish_message(const PubSubMessage *published) {
    PubSubMessage delivered = *published;
    Subscriber *subscriber;
    char frame[BUFFER_SIZE];
    int length;
    delivered.type = PUBSUB_MESSAGE;
    length = pubsub_encode(&delivered, frame, sizeof(frame));
    if (length < 0 || (size_t)length + 1 >= sizeof(frame)) return;
    frame[length++] = '\n';
    pthread_mutex_lock(&subscribers_mutex);
    for (subscriber = subscribers; subscriber != NULL;
         subscriber = subscriber->next) {
        if (subscriber_has_topic(subscriber, delivered.topic) &&
            subscriber_push(subscriber, frame, (size_t)length) != 0) {
            // si no se pudo encolar lo desconectamos
            shutdown(subscriber->socket_fd, SHUT_RDWR);
        }
    }
    pthread_mutex_unlock(&subscribers_mutex);
}
// saca al suscriptor de la lista
static void subscriber_remove(Subscriber *subscriber) {
    Subscriber **current = &subscribers;
    pthread_mutex_lock(&subscribers_mutex);
    while (*current != NULL && *current != subscriber) {
        current = &(*current)->next;
    }
    if (*current == subscriber) {
        *current = subscriber->next;
    }
    pthread_mutex_unlock(&subscribers_mutex);
}
// atiende a un publisher, cada linea que manda se reparte
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
// atiende a un suscriptor, lo registra y sigue leyendo por si pide mas temas
static void serve_subscriber(int socket_fd, const PubSubMessage *first_message) {
    Subscriber *subscriber;
    PubSubMessage message;
    char frame[BUFFER_SIZE];
    subscriber = subscriber_new(socket_fd, first_message->topic);
    if (subscriber == NULL) return;
    pthread_mutex_lock(&subscribers_mutex);
    subscriber->next = subscribers;
    subscribers = subscriber;
    pthread_mutex_unlock(&subscribers_mutex);
    printf("Subscriber registered for %s\n", first_message->topic);
    fflush(stdout);
    while (receive_line(socket_fd, frame, sizeof(frame)) == 1) {
        if (pubsub_decode(frame, &message) != 0 || message.type != PUBSUB_SUBSCRIBE) {
            break;
        }
        if (subscriber_add_topic(subscriber, message.topic) != 0) {
            break;
        }
    }
    // se fue el cliente, limpiamos todo
    subscriber_remove(subscriber);
    subscriber_close(subscriber);
    shutdown(socket_fd, SHUT_RDWR);
    pthread_join(subscriber->sender, NULL);
    subscriber_free(subscriber);
}
// aqui corre el hilo de cada conexion, lee el primer mensaje para saber si es publisher o suscriptor
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
// main
int main(void) {
    int server_fd;
    int reuse_address = 1;
    struct sockaddr_in address;
    // si escribimos a un cliente caido que no nos mate
    signal(SIGPIPE, SIG_IGN);
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
    // por cada cliente que llega se crea un hilo nuevo y se vuelve a esperar
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
        // un hilo por conexion
        if (pthread_create(&thread, NULL, client_worker, connection) != 0) {
            close(client_fd);
            free(connection);
            continue;
        }
        pthread_detach(thread);
    }
}

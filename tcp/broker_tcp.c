/* Solicita las declaraciones POSIX que este programa usa en Linux/Unix. */
#define _POSIX_C_SOURCE 200809L

/* IPv4, direcciones, hilos, sockets y entrada/salida del sistema operativo. */
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

/* Una entrada relaciona un tema con el socket TCP que lo pidio. */
typedef struct Subscription {
    int socket_fd;
    char topic[TOPIC_MAX_LEN];
    struct Subscription *next;
} Subscription;

/* Argumento entregado a cada hilo: identifica la conexion aceptada. */
typedef struct {
    int socket_fd;
} ClientConnection;

/* Lista global compartida por todos los hilos que atienden clientes. */
static Subscription *subscriptions;
/* Cerradura que permite que solo un hilo modifique/use la lista a la vez. */
static pthread_mutex_t subscribers_mutex = PTHREAD_MUTEX_INITIALIZER;

/* TCP puede enviar solo una parte; repetir hasta completar todos los bytes. */
static int send_all(int socket_fd, const char *buffer, size_t length) {
	/* sent cuenta cuanto del mensaje ya acepto el sistema operativo. */
    size_t sent = 0;

	/* Sigue enviando mientras falten bytes. */
    while (sent < length) {
        ssize_t count = send(socket_fd, buffer + sent, length - sent, 0);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            return -1;
        }
		/* Avanza exactamente la cantidad que send logro transmitir. */
        sent += (size_t)count;
    }
    return 0;
}

/* TCP es un flujo: esta funcion reconstruye un mensaje terminado en newline. */
static int receive_line(int socket_fd, char *buffer, size_t capacity) {
	/* used indica cuantos caracteres se han acumulado hasta ahora. */
    size_t used = 0;

	/* Lee de a un byte hasta newline, cierre o falta de espacio. */
    while (used + 1 < capacity) {
        char byte;
        ssize_t count = recv(socket_fd, &byte, 1, 0);
        if (count == 0) {
			/* Cero significa que el otro proceso cerro la conexion. */
            if (used == 0) {
                return 0;
            }
            break;
        }
        if (count < 0) {
			/* EINTR solo significa que una senal interrumpio la espera. */
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        buffer[used++] = byte;
		/* Newline termina el mensaje, no hace falta esperar mas datos. */
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

/* Agrega la relacion socket-tema una sola vez; el mutex protege la lista. */
static int add_subscription(int socket_fd, const char *name) {
    Subscription *subscription;

    /* Desde aqui ningun otro hilo puede cambiar subscriptions. */
    pthread_mutex_lock(&subscribers_mutex);
    for (subscription = subscriptions; subscription != NULL;
         subscription = subscription->next) {
        if (subscription->socket_fd == socket_fd &&
            strcmp(subscription->topic, name) == 0) {
            /* Ya estaba registrada: liberar el mutex antes de salir. */
            pthread_mutex_unlock(&subscribers_mutex);
            return 0;
        }
    }
    subscription = malloc(sizeof(*subscription));
    if (subscription == NULL) {
        pthread_mutex_unlock(&subscribers_mutex);
        return -1;
    }
    /* Inicializa la nueva entrada y la agrega al principio de la lista. */
    subscription->socket_fd = socket_fd;
    memcpy(subscription->topic, name, strlen(name) + 1);
    subscription->next = subscriptions;
    subscriptions = subscription;
    /* Todos los caminos de salida deben liberar el mutex. */
    pthread_mutex_unlock(&subscribers_mutex);
    return 0;
}

/* Convierte una publicacion en noticia y la reenvia a sus suscriptores. */
static void publish_message(const PubSubMessage *published) {
    /* Copia para cambiar el tipo sin modificar el mensaje original. */
    PubSubMessage delivered = *published;
    Subscription *subscription;
    char frame[BUFFER_SIZE];
    int length;

    /* PUB es lo que llega del publisher; M es lo que recibe el subscriber. */
    delivered.type = PUBSUB_MESSAGE;
    length = pubsub_encode(&delivered, frame, sizeof(frame));
    if (length < 0 || (size_t)length + 1 >= sizeof(frame)) {
        return;
    }
    /* TCP no conserva limites de mensajes: newline permite separarlos. */
    frame[length++] = '\n';

    // El mismo mutex evita escrituras simultaneas en una conexion TCP.
    /* El mutex tambien evita dos hilos escribiendo a la vez en un socket. */
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

/* Borra de la lista los temas de un subscriber que ya se desconecto. */
static void remove_subscriber(int socket_fd) {
    Subscription **current = &subscriptions;

    pthread_mutex_lock(&subscribers_mutex);
    /* current apunta al enlace que lleva a la entrada actual de la lista. */
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

/* Consume la primera publicacion y las siguientes enviadas por este publisher. */
static void serve_publisher(int socket_fd, const PubSubMessage *first_message) {
    PubSubMessage message = *first_message;
    char frame[BUFFER_SIZE];

    /* El primer mensaje ya se leyo en client_worker; luego se leen los demas. */
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

/* Registra temas; la conexion queda abierta para que lleguen noticias. */
static void serve_subscriber(int socket_fd, const PubSubMessage *first_message) {
    PubSubMessage message;
    char frame[BUFFER_SIZE];

    if (add_subscription(socket_fd, first_message->topic) != 0) {
        return;
    }
    printf("Subscriber registered for %s\n", first_message->topic);
    fflush(stdout);

    /* El primer tema se registro; las lineas siguientes pueden ser mas S|tema|. */
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

/* Cada hilo lee el primer mensaje para saber si atiende publisher o subscriber. */
static void *client_worker(void *argument) {
    ClientConnection *connection = argument;
    int socket_fd = connection->socket_fd;
    PubSubMessage first_message;
    char frame[BUFFER_SIZE];

    /* Copia el descriptor y libera el argumento que preparo el hilo principal. */
    free(connection);
    if (receive_line(socket_fd, frame, sizeof(frame)) == 1 &&
        pubsub_decode(frame, &first_message) == 0) {
        if (first_message.type == PUBSUB_PUBLISH) {
            serve_publisher(socket_fd, &first_message);
        } else if (first_message.type == PUBSUB_SUBSCRIBE) {
            serve_subscriber(socket_fd, &first_message);
        }
    }
    /* Al desconectarse, cierra el socket para liberar el recurso del sistema. */
    close(socket_fd);
    return NULL;
}

/* Crea el socket servidor y un hilo separado para cada cliente conectado. */
int main(void) {
    int server_fd;
    int reuse_address = 1;
    struct sockaddr_in address;

    /* Si se escribe a un cliente caido, send falla en vez de matar al broker. */
    signal(SIGPIPE, SIG_IGN);
    /* SOCK_STREAM significa TCP: conexion persistente y flujo confiable. */
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

    /* Prepara la direccion: acepta conexiones desde cualquier interfaz local. */
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
    /* accept espera clientes; el broker vuelve a aceptar al crear cada hilo. */
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
        /* El nuevo hilo sera dueno de este descriptor hasta cerrar la sesion. */
        connection->socket_fd = client_fd;
        if (pthread_create(&thread, NULL, client_worker, connection) != 0) {
            close(client_fd);
            free(connection);
            continue;
        }
        /* No se espera join: el hilo se limpia a si mismo al terminar. */
        pthread_detach(thread);
    }
}

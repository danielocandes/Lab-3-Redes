/*
 * subscriber_tcp.c
 * Subscriber (hincha) del sistema pub/sub sobre TCP.
 *
 * Responsabilidad (enunciado #4.2): suscribirse a uno o varios partidos y
 * recibir en pantalla las actualizaciones en vivo enviadas por el broker.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "pubsub_common.h"

int main(int argc, char *argv[]) {
    /* TODO 1: recibir por argv el/los topico(s) (partido) a los que este subscriber se suscribe */
    /* TODO 2: crear socket TCP y connect() a la IP:PORT_TCP del broker */
    /* TODO 3: enviar el mensaje inicial que identifica a este cliente como subscriber de ese topico */
    /* TODO 4: loop: recv()/read() de mensajes reenviados por el broker e imprimirlos en pantalla */
    /* TODO 5: cerrar el socket al terminar (ej. con Ctrl+C o una senal) */

    (void)argc;
    (void)argv;
    printf("subscriber_tcp: scaffold, falta implementar la recepcion de mensajes\n");
    return 0;
}

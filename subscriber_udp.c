/*
 * subscriber_udp.c
 * Subscriber (hincha) del sistema pub/sub sobre UDP.
 *
 * Misma responsabilidad que subscriber_tcp.c (enunciado #4.2): recibir en
 * pantalla las actualizaciones en vivo enviadas por el broker, esta vez por
 * datagramas independientes.
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
    /* TODO 2: crear socket UDP y bind() a un puerto local (para que el broker le pueda responder con sendto()) */
    /* TODO 3: enviar al broker un datagrama de suscripcion con el/los topico(s) */
    /* TODO 4: loop: recvfrom() de mensajes reenviados por el broker e imprimirlos en pantalla */
    /* TODO 5: cerrar el socket al terminar (ej. con Ctrl+C o una senal) */

    (void)argc;
    (void)argv;
    printf("subscriber_udp: scaffold, falta implementar la recepcion de mensajes\n");
    return 0;
}

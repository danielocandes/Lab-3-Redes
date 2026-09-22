/*
 * broker_udp.c
 * Broker del sistema de noticias deportivas (modelo pub/sub) sobre UDP.
 *
 * Misma responsabilidad que broker_tcp.c (enunciado #4.2), pero sin conexion:
 * cada publisher/subscriber es identificado por su struct sockaddr_in en cada
 * datagrama, no por un socket persistente.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "../pubsub_common.h"

int main(void) {
    /* TODO 1: crear socket UDP -> socket(AF_INET, SOCK_DGRAM, 0) */
    /* TODO 2: bind() a INADDR_ANY:PORT_UDP */
    /* TODO 3: loop de recvfrom() -- cada datagrama trae la direccion del remitente */
    /* TODO 4: distinguir por el contenido del datagrama si es un mensaje de un
     *         publisher (publicar en un topico) o una solicitud de suscripcion
     *         de un subscriber (usar el formato definido en pubsub_common.h)
     */
    /* TODO 5: mantener una estructura (ej. lista de topico -> lista de sockaddr_in de subscribers) */
    /* TODO 6: al llegar un mensaje de un publisher, reenviarlo con sendto() a cada
     *         subscriber suscrito a ese topico
     */
    /* TODO 7: sin conexion no hay forma nativa de detectar que un subscriber se
     *         fue -- documentar esta limitacion en el informe (comparacion con TCP)
     */

    printf("broker_udp: scaffold, falta implementar la logica del broker\n");
    return 0;
}

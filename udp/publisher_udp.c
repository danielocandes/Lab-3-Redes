/*
 * publisher_udp.c
 * Publisher (periodista deportivo) del sistema pub/sub sobre UDP.
 *
 * Misma responsabilidad que publisher_tcp.c (enunciado #4.2), enviando cada
 * mensaje como un datagrama independiente (sin conexion previa).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "../pubsub_common.h"

int main(int argc, char *argv[]) {
    /* TODO 1: recibir por argv el topico (partido) al que este publisher va a publicar */
    /* TODO 2: crear socket UDP (no hace falta connect(), aunque se puede usar para fijar el destino) */
    /* TODO 3: loop: sendto() al menos 10 mensajes (enunciado #6.3) con el formato
     *         topico+contenido acordado en pubsub_common.h
     */
    /* TODO 4: cerrar el socket al terminar */

    (void)argc;
    (void)argv;
    printf("publisher_udp: scaffold, falta implementar el envio de mensajes\n");
    return 0;
}

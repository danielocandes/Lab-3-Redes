/*
 * publisher_tcp.c
 * Publisher (periodista deportivo) del sistema pub/sub sobre TCP.
 *
 * Responsabilidad (enunciado #4.2): generar mensajes asociados a un topico
 * (partido) y enviarselos al broker. Ejemplos de mensaje:
 *   "Gol de Equipo A al minuto 32"
 *   "Cambio: jugador 10 entra por jugador 20"
 *   "Tarjeta amarilla al numero 10 de Equipo B"
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
    /* TODO 2: crear socket TCP y connect() a la IP:PORT_TCP del broker */
    /* TODO 3: enviar el mensaje inicial que identifica a este cliente como publisher del topico */
    /* TODO 4: loop: enviar al menos 10 mensajes (enunciado #6.3), con el formato
     *         topico+contenido acordado en pubsub_common.h
     */
    /* TODO 5: cerrar el socket al terminar */

    (void)argc;
    (void)argv;
    printf("publisher_tcp: scaffold, falta implementar el envio de mensajes\n");
    return 0;
}

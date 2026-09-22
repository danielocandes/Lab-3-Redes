/*
 * broker_tcp.c
 * Broker del sistema de noticias deportivas (modelo pub/sub) sobre TCP.
 *
 * Responsabilidades (enunciado #4.2):
 *   - Recibir mensajes de los publishers, cada uno asociado a un topico (partido).
 *   - Reenviar cada mensaje a los subscribers suscritos a ese topico.
 *   - No modificar el contenido de los mensajes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "../pubsub_common.h"

int main(void) {
    /* TODO 1: crear socket TCP -> socket(AF_INET, SOCK_STREAM, 0) */
    /* TODO 2: setsockopt(SO_REUSEADDR) para poder reiniciar el broker sin esperar el TIME_WAIT */
    /* TODO 3: bind() a INADDR_ANY:PORT_TCP */
    /* TODO 4: listen() para aceptar conexiones entrantes de publishers y subscribers */
    /* TODO 5: loop de accept() manejando multiples clientes a la vez (select/poll/epoll o un hilo por cliente) */
    /* TODO 6: por cada conexion nueva, leer el primer mensaje para distinguir si es
     *         un publisher (va a publicar en un topico) o un subscriber (se suscribe a uno)
     */
    /* TODO 7: mantener una estructura (ej. lista de topico -> lista de sockets suscriptores) */
    /* TODO 8: al llegar un mensaje de un publisher, reenviarlo (write/send) a cada
     *         subscriber suscrito a ese topico
     */
    /* TODO 9: manejar la desconexion de clientes (recv/read devuelve 0) sacandolos de las listas */

    printf("broker_tcp: scaffold, falta implementar la logica del broker\n");
    return 0;
}

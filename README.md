# Laboratorio 3 - Sockets (publisher/subscriber en C)

Broker, publishers y subscribers, una version TCP (puerto 5050) y otra UDP (puerto 5001).
Solo API estandar de sockets POSIX y `pthread`. Sin librerias externas.

## Requisitos

Linux o macOS con `gcc` y `make` instalados (en Ubuntu: `sudo apt install build-essential`).

## Compilar

    make          # todo (o make tcp / make udp). make clean borra los binarios

El Makefile compila cada `.c` con `gcc -Wall -Wextra -std=c11 -pthread`. `-pthread` es por los hilos del broker TCP.

## Ejecutar (una terminal por comando, en este orden)

    ./tcp/broker_tcp
    ./tcp/subscriber_tcp 127.0.0.1 partido1 partido2
    ./tcp/subscriber_tcp 127.0.0.1 partido1
    ./tcp/publisher_tcp 127.0.0.1 partido1 10
    ./tcp/publisher_tcp 127.0.0.1 partido2 10

Para UDP es igual cambiando `tcp` por `udp`.

## Pruebas

- Entrega: el subscriber 1 debe recibir 10 mensajes de cada partido y el 2 solo los de `partido1`.
- Perdida en UDP: `./udp/publisher_udp 127.0.0.1 partido1 100000` (en TCP llegan los 100000, en UDP no).
- Desorden en UDP: `./udp/broker_udp 500` retrasa 500 ms el primer mensaje (simulado). Cada mensaje trae `#n` para ver el orden.
- Wireshark: interfaz de loopback, filtros `tcp.port == 5050` y `udp.port == 5001`.

Formato de mensaje: una linea `TIPO|TEMA|CONTENIDO` (P publicar, S suscribir, M entrega del broker). En TCP termina en `\n`; en UDP cada datagrama es un mensaje.

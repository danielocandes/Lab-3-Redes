# Lab 3 - Redes

Repositorio del Laboratorio 3 del curso de Redes (ISIS-2311, Uniandes): sistema de noticias
deportivas en tiempo real con modelo publicador-suscriptor, en C, comparando TCP y UDP.

## Estructura

- `pubsub_common.h` — constantes y formato de mensaje compartidos por los 6 programas.
- `broker_tcp.c`, `publisher_tcp.c`, `subscriber_tcp.c` — versión TCP.
- `broker_udp.c`, `publisher_udp.c`, `subscriber_udp.c` — versión UDP.

Todos los `.c` son scaffold: compilan pero la lógica de sockets está marcada con `TODO`.

## Compilar

```
make        # los 6 binarios
make tcp    # solo broker_tcp, publisher_tcp, subscriber_tcp
make udp    # solo broker_udp, publisher_udp, subscriber_udp
make clean
```

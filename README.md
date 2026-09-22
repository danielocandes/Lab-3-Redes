# Lab 3 - Redes

Repositorio del Laboratorio 3 del curso de Redes (ISIS-2311, Uniandes): sistema de noticias
deportivas en tiempo real con modelo publicador-suscriptor, en C, comparando TCP y UDP.

## Estructura

- `pubsub_common.h` — constantes compartidas por los 6 programas (puertos, tamaños de buffer).
- `tcp/` — `broker_tcp.c`, `publisher_tcp.c`, `subscriber_tcp.c`.
- `udp/` — `broker_udp.c`, `publisher_udp.c`, `subscriber_udp.c`.

## Compilar

```
make        # los 6 binarios
make tcp    # solo broker_tcp, publisher_tcp, subscriber_tcp
make udp    # solo broker_udp, publisher_udp, subscriber_udp
make clean
```

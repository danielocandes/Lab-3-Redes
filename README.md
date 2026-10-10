# Laboratorio 3 - Sockets (publicador/suscriptor en C)

Sistema de noticias deportivas con broker, publishers y subscribers, una version con TCP y otra con UDP.
Solo se usa la API estandar de sockets de C/POSIX (`sys/socket.h`, `netinet/in.h`, `arpa/inet.h`) y `pthread`. No hay librerias externas.

## Archivos

- `pubsub_common.h`: puertos, limites y formato de los mensajes
- `tcp/`: `broker_tcp.c`, `publisher_tcp.c`, `subscriber_tcp.c`
- `udp/`: `broker_udp.c`, `publisher_udp.c`, `subscriber_udp.c`
- `scripts/run_demo.sh`: prueba automatica (broker, 2 subscribers, 2 publishers)

## Compilar

    make        # todo
    make tcp
    make udp
    make clean

## Ejecutar

Cada comando en una terminal distinta. Puertos: TCP 5050, UDP 5001.

    ./tcp/broker_tcp
    ./tcp/subscriber_tcp 127.0.0.1 futbol
    ./tcp/publisher_tcp 127.0.0.1 futbol 10

    ./udp/broker_udp
    ./udp/subscriber_udp 127.0.0.1 futbol
    ./udp/publisher_udp 127.0.0.1 futbol 10

El subscriber acepta varios temas. El publisher recibe el numero de mensajes (por defecto 10).
`./udp/broker_udp 500` retrasa 500 ms el primer mensaje para ver desorden (es simulado).

## Mensajes

Una linea de texto: `TIPO|TEMA|CONTENIDO`, con P (publicar), S (suscribir) y M (mensaje entregado por el broker).
En TCP cada mensaje termina en salto de linea. En UDP cada datagrama es un mensaje.

## Prueba automatica

    bash scripts/run_demo.sh tcp
    bash scripts/run_demo.sh udp 500

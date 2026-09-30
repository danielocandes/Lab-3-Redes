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
## Mensajes

El formato textual compartido es `TIPO|TEMA|CONTENIDO`: `P` publica, `S` suscribe y `M` entrega una noticia. En TCP cada mensaje termina en newline; en UDP cada datagrama contiene un mensaje. Un suscriptor puede indicar varios temas en la línea de comandos. Los temas no pueden contener `|` ni saltos de línea; el contenido tampoco puede contener saltos de línea.

El broker TCP crea un hilo por conexión. Para este laboratorio reenvía directamente y usa un mutex para que dos hilos no escriban al mismo tiempo al mismo subscriber.

El broker UDP usa un solo loop de recepción; solo crea un hilo temporal para retrasar la primera entrega. Las notas del laboratorio no aclaran si UDP también debe tener un hilo por subscriber, así que confirmen ese punto con los monitores.

## Ejecutar TCP

En terminales separadas, desde la raíz del repositorio:

```bash
./tcp/broker_tcp
./tcp/subscriber_tcp 127.0.0.1 futbol
./tcp/publisher_tcp 127.0.0.1 futbol 10
```

El suscriptor permanece activo hasta que se interrumpa. El publisher acepta un conteo opcional de mensajes, cuyo valor predeterminado es 10.

## Ejecutar UDP

En terminales separadas:

```bash
./udp/broker_udp 500
./udp/subscriber_udp 127.0.0.1 futbol
./udp/publisher_udp 127.0.0.1 futbol 10
```

El argumento del broker UDP es un retraso opcional en milisegundos para la primera publicación recibida después de iniciar el broker. Mientras ese mensaje espera, las siguientes publicaciones se reenvían de inmediato, permitiendo observar reordenamiento controlado. Esto demuestra el efecto del retraso y no una garantía de orden o pérdida propia de toda red UDP.

## Prueba Repetible

En Linux o WSL, ejecute desde la raíz:

```bash
bash scripts/run_demo.sh tcp
bash scripts/run_demo.sh udp 500
```

El script compila el transporte, inicia un broker, dos suscriptores en temas distintos y dos publishers de diez mensajes, comprueba el filtrado y limpia los procesos. Para UDP también verifica que el primer mensaje retrasado llegue después de los siguientes.

# Lab 3 - Redes

Repositorio del Laboratorio 3 del curso de Redes (ISIS-2311, Uniandes): sistema de noticias
deportivas en tiempo real con modelo publicador-suscriptor en C. TCP y UDP cubren la guia; QUIC
se agrega en C como transporte adicional de bono usando la biblioteca MsQuic.

## Estructura

- `pubsub_common.h` — constantes y formato de mensajes compartidos.
- `tcp/` — `broker_tcp.c`, `publisher_tcp.c`, `subscriber_tcp.c`.
- `udp/` — `broker_udp.c`, `publisher_udp.c`, `subscriber_udp.c`.
- `quic/` — broker, publisher y subscriber QUIC en C sobre MsQuic.
- `GUIA_CODIGO.md` — introduccion a los protocolos, archivos `.h` y flujo del programa.

Si es la primera vez que lees el proyecto, empieza por [la guia de conceptos y codigo](GUIA_CODIGO.md).

## Compilar

```
make        # los 6 binarios
make tcp    # solo broker_tcp, publisher_tcp, subscriber_tcp
make udp    # solo broker_udp, publisher_udp, subscriber_udp
make quic   # los 3 binarios QUIC; requiere MsQuic
make clean
```

QUIC no forma parte de `make all`, para que la dependencia opcional no afecte TCP ni UDP. Para
compilarlo se necesita MsQuic (biblioteca y encabezados de desarrollo) instalado en el sistema;
consulte la guia oficial de [compilacion e instalacion de MsQuic](https://github.com/microsoft/msquic/blob/main/docs/BUILD.md).

## Mensajes

El formato textual compartido es `TIPO|TEMA|CONTENIDO`: `P` publica, `S` suscribe y `M` entrega una noticia. En TCP cada mensaje termina en newline; en UDP cada datagrama contiene un mensaje. Un suscriptor puede indicar varios temas en la línea de comandos. Los temas no pueden contener `|` ni saltos de línea; el contenido tampoco puede contener saltos de línea.

El broker TCP crea un hilo por conexión. Para este laboratorio reenvía directamente y usa un mutex para que dos hilos no escriban al mismo tiempo al mismo subscriber.

El broker UDP usa un solo loop de recepción; solo crea un hilo temporal para retrasar la primera entrega. Las notas del laboratorio no aclaran si UDP también debe tener un hilo por subscriber, así que confirmen ese punto con los monitores.

## Ejecutar QUIC (bono)

Instale OpenSSL y MsQuic, compile con `make quic` y genere un certificado de prueba desde la raiz:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout quic-key.pem \
	-out quic-cert.pem -days 1 -subj "/CN=localhost"
```

En terminales separadas:

```bash
./quic/broker_quic quic-cert.pem quic-key.pem
./quic/subscriber_quic 127.0.0.1 futbol
./quic/publisher_quic 127.0.0.1 futbol 10
```

El broker escucha en UDP 5002; cada cliente usa un stream bidireccional QUIC. El suscriptor acepta
varios temas como argumentos. Esta demo desactiva la validacion del certificado en el cliente para
evitar pasos extra de PKI; use un certificado de confianza fuera del laboratorio.

La prueba reproducible levanta broker, dos suscriptores y dos publishers, y comprueba entrega y
filtrado:

```bash
bash scripts/run_quic_demo.sh
```

En WSL/Ubuntu, instale primero MsQuic, sus encabezados de desarrollo y OpenSSL siguiendo la guia
oficial enlazada arriba. En Windows nativo estos fuentes requieren adaptar la capa POSIX usada por
los programas existentes; el flujo de trabajo del repositorio es Linux/WSL.

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

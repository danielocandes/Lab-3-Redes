# Guia Para Entender El Codigo

Esta guia acompana los comentarios de los fuentes. No hace falta aprender todo C
antes de empezar: primero entiende quien habla con quien y despues sigue una
ejecucion de principio a fin.

## 1. La idea del laboratorio

El programa es un sistema de noticias por temas, llamado publicador-suscriptor:

- El **publisher** produce una noticia y se la envia al broker.
- El **subscriber** le dice al broker que temas quiere recibir.
- El **broker** recuerda las suscripciones y reenvia cada noticia solo a quienes
  eligieron su tema.

Por ejemplo, un publisher envia `P|futbol|Gol del equipo local`. El broker busca
quien esta suscrito a `futbol` y le entrega `M|futbol|Gol del equipo local`.
La letra cambia porque una cosa es publicar y otra es recibir una noticia.

## 2. Que son TCP, UDP y QUIC

Los tres son formas de llevar bytes entre programas por una red. El contenido
del proyecto (tema y noticia) es el mismo; cambia el transporte.

| Transporte | Imagen mental | Lo que debe resolver el programa |
| --- | --- | --- |
| TCP | Una llamada conectada que queda abierta | TCP garantiza entrega ordenada de bytes, pero no conserva los limites de cada mensaje. Por eso el proyecto termina cada linea con `\n`. |
| UDP | Enviar cartas sueltas con una direccion en cada una | Cada envio es un datagrama independiente. Puede perderse o llegar fuera de orden; el broker aprende la direccion UDP del subscriber a partir de su suscripcion. |
| QUIC | Una conexion segura con varios canales independientes | QUIC usa UDP por debajo, pero agrega handshake TLS, entrega fiable y streams. El codigo usa MsQuic para implementar el protocolo; no crea QUIC a mano. |

**TCP no es “mas rapido” por definicion y UDP no reordena siempre.** La prueba
UDP del laboratorio retrasa intencionalmente una publicacion para que el efecto
se vea de forma repetible; no pretende afirmar que toda red UDP desordene cada
mensaje.

## 3. Que significa `.h` y que significa `.c`

- Un archivo `.c` contiene instrucciones que se compilan: por ejemplo, como
  abrir un socket o como reenviar una noticia.
- Un archivo `.h` es un encabezado: publica definiciones que varios `.c`
  necesitan compartir. No es un programa independiente.
- `#include "../pubsub_common.h"` copia esas definiciones al compilar ese archivo.
  Asi TCP, UDP y QUIC usan los mismos tipos de mensaje y limites.
- `pubsub_common.h` contiene `PubSubMessage`, los puertos y las funciones que
  convierten entre una estructura de C y el texto que viaja por la red.
- `quic/quic_client_common.h` agrupa las llamadas repetidas a MsQuic que usan el
  publisher y el subscriber QUIC.

El texto `#include <...>` normalmente trae una biblioteca del sistema o externa.
El texto `#include "..."` normalmente incluye un archivo del propio proyecto.

## 4. Como leer un mensaje

El formato de aplicacion es:

```text
TIPO|TEMA|CONTENIDO
```

- `P`: el publisher publica.
- `S`: el subscriber se suscribe; el contenido queda vacio.
- `M`: el broker entrega una noticia.
- `|`: separa los tres campos.
- `\n`: separa lineas en TCP y en los streams QUIC. No se agrega a los
  datagramas UDP porque cada datagrama ya marca el limite del mensaje.

No se puede usar `|` en los temas porque el decoder lo interpreta como
separador. Los arreglos tienen limites fijos para impedir que un mensaje muy
largo escriba fuera de la memoria reservada.

## 5. Responsabilidad de cada archivo

| Archivo | Responsabilidad |
| --- | --- |
| `pubsub_common.h` | Puertos, tamanos, tipos, codificacion y decodificacion compartidos. |
| `tcp/broker_tcp.c` | Acepta conexiones TCP, crea un hilo por cliente y distribuye noticias con un mutex para proteger datos compartidos. |
| `tcp/publisher_tcp.c` | Abre TCP al broker y envia publicaciones separadas por newline. |
| `tcp/subscriber_tcp.c` | Abre TCP, envia sus temas y muestra cada noticia recibida. |
| `udp/broker_udp.c` | Recibe suscripciones/publicaciones en datagramas, recuerda IP y puerto y reenvia por tema. El hilo de demora existe para el requisito de reordenamiento. |
| `udp/publisher_udp.c` | Envia una publicacion por datagrama UDP. |
| `udp/subscriber_udp.c` | Reserva un puerto local, anuncia sus temas y espera datagramas del broker. |
| `quic/broker_quic.c` | Abre listener MsQuic/TLS, acepta conexiones y streams, y distribuye publicaciones. |
| `quic/publisher_quic.c` | Abre un stream QUIC y envia publicaciones. |
| `quic/subscriber_quic.c` | Envia suscripciones por un stream y muestra las noticias que llegan. |
| `quic/quic_client_common.h` | Ayudantes y estado compartido por los dos clientes QUIC. |
| `Makefile` | Reglas para compilar los binarios; QUIC es optativo porque necesita MsQuic. |
| `scripts/run_demo.sh` | Ejecuta automaticamente la prueba de entrega/filtrado TCP o UDP y limpia procesos/logs. |
| `scripts/run_quic_demo.sh` | Hace lo mismo para QUIC y crea un certificado temporal de prueba. |

## 6. Hilos, mutex y callbacks

En TCP, `accept()` devuelve una conexion nueva. El broker crea un hilo para que
un cliente lento no impida atender a los demas. Los hilos comparten la lista de
suscripciones; un **mutex** funciona como una llave: solo un hilo entra a la
seccion protegida a la vez.

UDP no tiene conexiones que aceptar. El broker recibe datagramas en un ciclo.
El hilo auxiliar de demora espera antes de enviar solo el primer mensaje; el
ciclo principal puede seguir recibiendo los demas y por eso llegan antes.

MsQuic trabaja con **callbacks**: en lugar de llamar `recv()` repetidamente, el
programa registra funciones para que la biblioteca las invoque cuando ocurre un
evento (conexion lista, llegaron bytes, envio terminado o stream cerrado). El
mutex y la condicion de espera coordinan esos callbacks con el hilo principal.

## 7. Lo que se dejo y lo que se evito agregar

Se conservaron el broker concurrente TCP, el reordenamiento controlado UDP, las
validaciones de tamanos/formato y el soporte QUIC opcional: todos tienen un
proposito en la guia o evitan errores de red/memoria. No se agregaron colas,
historial de noticias, reconexion automatica, autenticacion de usuarios ni
persistencia, porque no son necesarios para el pub/sub basico del laboratorio.

La demo QUIC acepta un certificado autofirmado para facilitar la prueba local.
Eso no debe copiarse a una aplicacion desplegada: alli se valida la identidad del
servidor con certificados de confianza.

## 8. Orden recomendado para estudiar

1. Lee `pubsub_common.h`: entiende estructura y formato antes de mirar sockets.
2. Lee `tcp/publisher_tcp.c` y `tcp/subscriber_tcp.c`: son los clientes mas
   directos.
3. Lee `tcp/broker_tcp.c`: sigue `accept` -> hilo -> primer mensaje -> publicar
   o suscribir.
4. Compara los tres archivos de `udp/`: identifica que cambia al reemplazar
   conexion persistente por datagramas.
5. Deja `quic/` para el final: el modelo de mensajes sigue igual, pero MsQuic
   introduce handles, callbacks, TLS y streams.

Para probar TCP y UDP en Linux/WSL desde la raiz del proyecto:

```bash
bash scripts/run_demo.sh tcp
bash scripts/run_demo.sh udp 500
```

QUIC es un bono optativo y requiere MsQuic instalado:

```bash
make quic
bash scripts/run_quic_demo.sh
```
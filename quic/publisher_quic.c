/* Habilita interfaces POSIX que usa este cliente en Linux/Unix. */
#define _POSIX_C_SOURCE 200809L

/* Bibliotecas para argumentos, mensajes de consola, cadenas y PID del proceso. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "quic_client_common.h"

/* Conecta con el broker y publica una secuencia de mensajes QUIC. */
int main(int argc, char **argv) {
	QuicClient client;
	long message_count = 10;
	char *end;
	int result = EXIT_FAILURE;

	/* Espera host, tema y opcionalmente cuantos mensajes publicar. */
	if (argc < 3 || argc > 4 || argv[2][0] == '\0' ||
		strlen(argv[2]) >= TOPIC_MAX_LEN || strpbrk(argv[2], "|\r\n") != NULL) {
		fprintf(stderr, "Usage: %s <broker-host> <topic> [message-count]\n", argv[0]);
		return EXIT_FAILURE;
	}
	/* Si no se especifica una cantidad, usa diez publicaciones. */
	if (argc == 4) {
		message_count = strtol(argv[3], &end, 10);
		if (*argv[3] == '\0' || *end != '\0' || message_count < 1) {
			fprintf(stderr, "message-count must be a positive integer\n");
			return EXIT_FAILURE;
		}
	}
	/* El helper abre MsQuic, negocia TLS y espera que la conexion este lista. */
	if (quic_client_connect(&client, argv[1], PORT_QUIC) != 0) goto cleanup;
	if (quic_client_open_stream(&client, quic_client_stream_callback) != 0) {
		fprintf(stderr, "Could not open QUIC stream\n");
		goto cleanup;
	}
	/* Un stream lleva varias lineas, una por noticia publicada. */
	for (long sequence = 1; sequence <= message_count; sequence++) {
		PubSubMessage message = { .type = PUBSUB_PUBLISH };
		memcpy(message.topic, argv[2], strlen(argv[2]) + 1);
		if (pubsub_format_sports_event(message.content, sizeof(message.content),
										 argv[2], (long)getpid(), sequence, message_count) < 0) {
			fprintf(stderr, "Could not create sports event\n");
			goto cleanup;
		}
		/* Solo la ultima linea lleva FIN, que cierra el envio del publisher. */
		if (quic_client_send_message(&client, &message, sequence == message_count) != 0) {
			fprintf(stderr, "Could not send QUIC publication\n");
			goto cleanup;
		}
	}
	result = EXIT_SUCCESS;

cleanup:
	/* La etiqueta permite cerrar recursos tanto ante exito como ante error. */
	quic_client_close(&client);
	return result;
}

/* Habilita funciones POSIX para senales y espera temporizada. */
#define _POSIX_C_SOURCE 200809L

#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "quic_client_common.h"

/* La senal solo escribe esta bandera; el programa cierra QUIC fuera del handler. */
static volatile sig_atomic_t stop_requested;

/* Ctrl+C indica al bucle principal que debe terminar ordenadamente. */
static void request_stop(int signal_number) {
	(void)signal_number;
	stop_requested = 1;
}

/* Callback que reconstruye lineas QUIC y muestra las noticias recibidas. */
static QUIC_STATUS QUIC_API subscriber_stream_callback(
	HQUIC stream, void *context, QUIC_STREAM_EVENT *event) {
	QuicClient *client = context;
	/* Delega los eventos de envio/cierre al helper comun del cliente. */
	if (event->Type != QUIC_STREAM_EVENT_RECEIVE) {
		return quic_client_stream_callback(stream, context, event);
	}
	{
		/* El stream es un flujo: guarda bytes entre callbacks hasta newline. */
		char *frame = client->receive_frame;
		size_t *used = &client->receive_used;
		for (uint32_t buffer_index = 0; buffer_index < event->RECEIVE.BufferCount;
			 buffer_index++) {
			QUIC_BUFFER *buffer = &event->RECEIVE.Buffers[buffer_index];
			for (uint32_t byte_index = 0; byte_index < buffer->Length; byte_index++) {
				char byte = (char)buffer->Buffer[byte_index];
				/* Si una trama es demasiado larga, la descarta sin desbordar memoria. */
				if (*used + 1 >= sizeof(client->receive_frame)) {
					*used = 0;
					continue;
				}
				frame[(*used)++] = byte;
				/* Newline indica mensaje completo: ya se puede decodificar y mostrar. */
				if (byte == '\n') {
					PubSubMessage message;
					frame[*used] = '\0';
					if (pubsub_decode(frame, &message) == 0 &&
						message.type == PUBSUB_MESSAGE) {
						printf("[%s] %s\n", message.topic, message.content);
						fflush(stdout);
					}
					*used = 0;
				}
			}
		}
	}
	(void)client;
	return QUIC_STATUS_SUCCESS;
}

/* Se suscribe a varios temas y mantiene el stream abierto para noticias. */
int main(int argc, char **argv) {
	QuicClient client;
	struct timespec interval = { .tv_sec = 0, .tv_nsec = 200000000L };
	if (argc < 3) {
		fprintf(stderr, "Usage: %s <broker-host> <topic> [topic ...]\n", argv[0]);
		return EXIT_FAILURE;
	}
	/* Valida todos los temas antes de iniciar la conexion de red. */
	for (int index = 2; index < argc; index++) {
		if (argv[index][0] == '\0' || strlen(argv[index]) >= TOPIC_MAX_LEN ||
			strpbrk(argv[index], "|\r\n") != NULL) {
			fprintf(stderr, "Invalid topic: %s\n", argv[index]);
			return EXIT_FAILURE;
		}
	}
	/* Negocia QUIC/TLS con el broker indicado por el usuario. */
	if (quic_client_connect(&client, argv[1], PORT_QUIC) != 0) {
		quic_client_close(&client);
		return EXIT_FAILURE;
	}
	if (quic_client_open_stream(&client, subscriber_stream_callback) != 0) {
		fprintf(stderr, "Could not open QUIC stream\n");
		quic_client_close(&client);
		return EXIT_FAILURE;
	}
	/* Envia una trama de suscripcion S por cada tema solicitado. */
	for (int index = 2; index < argc; index++) {
		PubSubMessage message = { .type = PUBSUB_SUBSCRIBE };
		memcpy(message.topic, argv[index], strlen(argv[index]) + 1);
		if (quic_client_send_message(&client, &message, 0) != 0) {
			fprintf(stderr, "Could not send QUIC subscription\n");
			quic_client_close(&client);
			return EXIT_FAILURE;
		}
	}
	printf("Subscribed to %d topic(s); press Ctrl+C to stop\n", argc - 2);
	fflush(stdout);
	/* Captura Ctrl+C/Ctrl+Break para salir del bucle de espera. */
	signal(SIGINT, request_stop);
	signal(SIGTERM, request_stop);
	/* Espera en pausas cortas sin gastar CPU mientras llegan callbacks QUIC. */
	while (!stop_requested) nanosleep(&interval, NULL);
	quic_client_close(&client);
	return EXIT_SUCCESS;
}
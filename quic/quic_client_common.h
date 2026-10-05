/*
 * Encabezado .h compartido por publisher_quic.c y subscriber_quic.c.
 * Reune funciones comunes para conectar, abrir un stream, enviar y cerrar.
 */
#ifndef QUIC_CLIENT_COMMON_H
#define QUIC_CLIENT_COMMON_H

/* MsQuic implementa QUIC; pthread sincroniza el programa con sus callbacks. */
#include <msquic.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../pubsub_common.h"

/* ALPN identifica el protocolo de aplicacion negociado durante el handshake. */
#define QUIC_ALPN "lab3-pubsub"

/* Recursos QUIC y estado compartido entre el programa y callbacks de MsQuic. */
typedef struct {
	/* Tabla de funciones y handles que representan objetos creados por MsQuic. */
	const QUIC_API_TABLE *api;
	HQUIC registration;
	HQUIC configuration;
	HQUIC connection;
	HQUIC stream;
	/* Mutex y condicion sirven para esperar avisos sin revisar en un bucle activo. */
	pthread_mutex_t mutex;
	pthread_cond_t changed;
	/* Banderas sincronizadas con mutex: conexion, stream y envio. */
	int connected;
	int connection_closed;
	int stream_closed;
	int send_complete;
	/* QUIC es un flujo: acumula bytes hasta completar una linea del protocolo. */
	char receive_frame[BUFFER_SIZE];
	size_t receive_used;
} QuicClient;

/* MsQuic conserva este buffer hasta avisar que termino de enviarlo. */
typedef struct {
	QUIC_BUFFER buffer;
	char bytes[BUFFER_SIZE];
} QuicSendBuffer;

/* MsQuic llama esta funcion cuando la conexion cambia de estado. */
static QUIC_STATUS QUIC_API quic_client_connection_callback(
	HQUIC connection, void *context, QUIC_CONNECTION_EVENT *event) {
	QuicClient *client = context;
	(void)connection;
	/* Protege las banderas que tambien consulta el hilo principal. */
	pthread_mutex_lock(&client->mutex);
	if (event->Type == QUIC_CONNECTION_EVENT_CONNECTED) {
		/* El handshake TLS/QUIC termino; el cliente ya puede abrir streams. */
		client->connected = 1;
	} else if (event->Type == QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) {
		/* MsQuic termino el cierre; se puede liberar el handle de conexion. */
		client->connection_closed = 1;
	}
	/* Despierta a quien este esperando conexion o cierre. */
	pthread_cond_broadcast(&client->changed);
	pthread_mutex_unlock(&client->mutex);
	return QUIC_STATUS_SUCCESS;
}

/* Confirma que MsQuic ya no necesita el buffer y que el stream termino de cerrar. */
static QUIC_STATUS QUIC_API quic_client_stream_callback(
	HQUIC stream, void *context, QUIC_STREAM_EVENT *event) {
	QuicClient *client = context;
	if (event->Type == QUIC_STREAM_EVENT_SEND_COMPLETE) {
		/* El contexto es el buffer que se reservo antes de StreamSend. */
		free(event->SEND_COMPLETE.ClientContext);
		pthread_mutex_lock(&client->mutex);
		client->send_complete = 1;
		pthread_cond_broadcast(&client->changed);
		pthread_mutex_unlock(&client->mutex);
	} else if (event->Type == QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE) {
		/* Cierra el handle solo cuando MsQuic notifica que el stream termino. */
		client->api->StreamClose(stream);
		pthread_mutex_lock(&client->mutex);
		client->stream_closed = 1;
		client->stream = NULL;
		pthread_cond_broadcast(&client->changed);
		pthread_mutex_unlock(&client->mutex);
	}
	return QUIC_STATUS_SUCCESS;
}

/* Abre MsQuic, configura la conexion TLS y espera al handshake. */
static int quic_client_connect(QuicClient *client, const char *host, uint16_t port) {
	/* Perfil de trabajo y nombre de protocolo de aplicacion. */
	QUIC_REGISTRATION_CONFIG registration_config = {
		"lab3-pubsub-client", QUIC_EXECUTION_PROFILE_LOW_LATENCY
	};
	QUIC_BUFFER alpn = { (uint32_t)(sizeof(QUIC_ALPN) - 1), (uint8_t *)QUIC_ALPN };
	/* Datos de TLS; la demo de clase acepta certificado autofirmado. */
	QUIC_CREDENTIAL_CONFIG credentials = {0};
	QUIC_STATUS status;

	/* Inicializa handles/contadores y prepara mutex y condicion de espera. */
	memset(client, 0, sizeof(*client));
	pthread_mutex_init(&client->mutex, NULL);
	pthread_cond_init(&client->changed, NULL);
	/* Carga la API de MsQuic y obtiene las funciones para crear recursos. */
	status = MsQuicOpen2(&client->api);
	if (QUIC_FAILED(status)) goto fail;
	/* Registration agrupa los objetos QUIC pertenecientes a este cliente. */
	status = client->api->RegistrationOpen(&registration_config, &client->registration);
	if (QUIC_FAILED(status)) goto fail;
	/* Configuration guarda ALPN y opciones; luego se asocia el modo TLS. */
	status = client->api->ConfigurationOpen(client->registration, &alpn, 1, NULL, 0,
													 NULL, &client->configuration);
	if (QUIC_FAILED(status)) goto fail;
	/* El cliente no presenta certificado; para la demo tampoco valida el servidor. */
	credentials.Type = QUIC_CREDENTIAL_TYPE_NONE;
	credentials.Flags = QUIC_CREDENTIAL_FLAG_CLIENT |
		QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
	status = client->api->ConfigurationLoadCredential(client->configuration, &credentials);
	if (QUIC_FAILED(status)) goto fail;
	/* Crea la conexion y registra la funcion que recibira sus eventos. */
	status = client->api->ConnectionOpen(client->registration,
												 quic_client_connection_callback, client,
												 &client->connection);
	if (QUIC_FAILED(status)) goto fail;
	/* Inicia la conexion hacia el host y puerto del broker. */
	status = client->api->ConnectionStart(client->connection, client->configuration,
												 QUIC_ADDRESS_FAMILY_UNSPEC, host, port);
	if (QUIC_FAILED(status)) goto fail;
	pthread_mutex_lock(&client->mutex);
	/* Espera dormido hasta que el callback indique conexion o fallo/cierre. */
	while (!client->connected && !client->connection_closed)
		pthread_cond_wait(&client->changed, &client->mutex);
	pthread_mutex_unlock(&client->mutex);
	if (!client->connected) goto fail;
	return 0;

fail:
	/* Muestra el codigo que devolvio la llamada de MsQuic que fallo. */
	fprintf(stderr, "QUIC client setup failed (0x%x)\n", (unsigned int)status);
	return -1;
}

/* Crea un stream bidireccional para llevar mensajes de aplicacion. */
static int quic_client_open_stream(QuicClient *client,
								   QUIC_STREAM_CALLBACK_HANDLER callback) {
	QUIC_STATUS status = client->api->StreamOpen(client->connection,
													 QUIC_STREAM_OPEN_FLAG_NONE,
													 callback, client, &client->stream);
	if (QUIC_FAILED(status)) return -1;
	/* Start pone el stream en marcha y permite que viaje por la red. */
	status = client->api->StreamStart(client->stream, QUIC_STREAM_START_FLAG_IMMEDIATE);
	return QUIC_FAILED(status) ? -1 : 0;
}

/* Encola un mensaje y espera que MsQuic permita liberar la memoria usada. */
static int quic_client_send(QuicClient *client, const char *frame, size_t length,
							int finish_stream) {
	/* El buffer vive en heap porque MsQuic lo usa despues de retornar StreamSend. */
	QuicSendBuffer *send_buffer = malloc(sizeof(*send_buffer));
	QUIC_STATUS status;
	if (send_buffer == NULL || length > sizeof(send_buffer->bytes)) {
		free(send_buffer);
		return -1;
	}
	memcpy(send_buffer->bytes, frame, length);
	send_buffer->buffer.Length = (uint32_t)length;
	send_buffer->buffer.Buffer = (uint8_t *)send_buffer->bytes;
	pthread_mutex_lock(&client->mutex);
	client->send_complete = 0;
	pthread_mutex_unlock(&client->mutex);
	/* FIN comunica que el publisher no enviara mas datos por este stream. */
	status = client->api->StreamSend(client->stream, &send_buffer->buffer, 1,
									 finish_stream ? QUIC_SEND_FLAG_FIN : QUIC_SEND_FLAG_NONE,
									 send_buffer);
	if (QUIC_FAILED(status)) {
		free(send_buffer);
		return -1;
	}
	pthread_mutex_lock(&client->mutex);
	/* El callback cambia send_complete al recibir SEND_COMPLETE. */
	while (!client->send_complete && !client->connection_closed)
		pthread_cond_wait(&client->changed, &client->mutex);
	int completed = client->send_complete;
	pthread_mutex_unlock(&client->mutex);
	return completed ? 0 : -1;
}

/* Cierra ordenadamente la conexion y libera todos los handles abiertos. */
static void quic_client_close(QuicClient *client) {
	if (client->connection != NULL) {
		/* Pide cierre a MsQuic y espera su confirmacion antes de liberar. */
		client->api->ConnectionShutdown(client->connection,
											QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
		pthread_mutex_lock(&client->mutex);
		while (!client->connection_closed)
			pthread_cond_wait(&client->changed, &client->mutex);
		pthread_mutex_unlock(&client->mutex);
		client->api->ConnectionClose(client->connection);
	}
	/* Libera objetos en orden inverso al de creacion. */
	if (client->configuration != NULL) client->api->ConfigurationClose(client->configuration);
	if (client->registration != NULL) client->api->RegistrationClose(client->registration);
	if (client->api != NULL) MsQuicClose(client->api);
	pthread_cond_destroy(&client->changed);
	pthread_mutex_destroy(&client->mutex);
}

/* Codifica una estructura a texto y agrega newline para separar las tramas. */
static int quic_client_send_message(QuicClient *client, PubSubMessage *message,
									int finish_stream) {
	char frame[BUFFER_SIZE];
	int length = pubsub_encode(message, frame, sizeof(frame));
	if (length < 0 || (size_t)length + 1 >= sizeof(frame)) return -1;
	frame[length++] = '\n';
	if (quic_client_send(client, frame, (size_t)length, finish_stream) != 0)
		return -1;
	if (finish_stream) {
		/* El publisher espera a que el broker cierre su lado del stream. */
		pthread_mutex_lock(&client->mutex);
		while (!client->stream_closed && !client->connection_closed)
			pthread_cond_wait(&client->changed, &client->mutex);
		int stream_closed = client->stream_closed;
		pthread_mutex_unlock(&client->mutex);
		return stream_closed ? 0 : -1;
	}
	return 0;
}

#endif
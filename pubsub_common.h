/*
 * Este archivo .h es un encabezado: comparte definiciones entre varios .c.
 * Asi TCP, UDP y QUIC usan los mismos puertos, limites y formato de mensaje.
 */
#ifndef PUBSUB_COMMON_H
#define PUBSUB_COMMON_H

/* Bibliotecas usadas por tamanos, formato de texto y funciones de cadenas. */
#include <stddef.h>
#include <stdio.h>
#include <string.h>

/* Cada transporte escucha en un puerto distinto del mismo computador. */
#define PORT_TCP        5000
#define PORT_UDP        5001
#define PORT_QUIC       5002

/* Los arreglos tienen limites fijos para evitar mensajes ilimitados. */
#define TOPIC_MAX_LEN   64
#define MSG_MAX_LEN     256
/* Espacio para tipo, separadores y terminadores del formato de texto. */
#define BUFFER_SIZE     (TOPIC_MAX_LEN + MSG_MAX_LEN + 16)

/* Un mensaje puede pedir publicar, suscribirse o entregar una noticia. */
typedef enum {
	PUBSUB_PUBLISH = 'P',
	PUBSUB_SUBSCRIBE = 'S',
	PUBSUB_MESSAGE = 'M'
} PubSubMessageType;

/* Representacion del mensaje dentro del programa, antes de enviarlo. */
typedef struct {
	PubSubMessageType type;
	char topic[TOPIC_MAX_LEN];
	char content[MSG_MAX_LEN];
} PubSubMessage;

/*
 * Convierte la estructura a "TIPO|TEMA|CONTENIDO".
 * Devuelve cuantos caracteres escribio, o -1 si los datos no son validos.
 */
static inline int pubsub_encode(const PubSubMessage *message, char *buffer,
								size_t buffer_size) {
	/* snprintf necesita que recordemos cuantos caracteres produjo. */
	int length;

	/* Rechaza punteros vacios, buffers vacios y tipos desconocidos. */
	if (message == NULL || buffer == NULL || buffer_size == 0 ||
		(message->type != PUBSUB_PUBLISH &&
		 message->type != PUBSUB_SUBSCRIBE &&
		 message->type != PUBSUB_MESSAGE)) {
		return -1;
	}
	/* Comprueba terminadores y evita caracteres que romperian el formato. */
	if (memchr(message->topic, '\0', sizeof(message->topic)) == NULL ||
		memchr(message->content, '\0', sizeof(message->content)) == NULL ||
		message->topic[0] == '\0' ||
		strpbrk(message->topic, "|\r\n") != NULL ||
		strpbrk(message->content, "\r\n") != NULL ||
		(message->type == PUBSUB_SUBSCRIBE && message->content[0] != '\0')) {
		return -1;
	}
	/* Escribe tipo, tema y contenido separados por el caracter |. */
	length = snprintf(buffer, buffer_size, "%c|%s|%s", (char)message->type,
					  message->topic, message->content);
	/* snprintf devuelve un tamano que no cabia si es igual o mayor al limite. */
	if (length < 0 || (size_t)length >= buffer_size) {
		return -1;
	}

	return length;
}

/*
 * Hace la operacion inversa: toma el texto recibido y llena la estructura.
 * El transporte debe entregar una linea completa o un datagrama completo.
 */
static inline int pubsub_decode(const char *buffer, PubSubMessage *message) {
	/* Punteros a los separadores; no se copia texto antes de validar. */
	const char *separator;
	const char *content;
	size_t topic_length;
	size_t content_length;
	size_t frame_length;

	if (buffer == NULL || message == NULL) {
		return -1;
	}
	/* Newline puede estar presente en TCP/QUIC; no es parte del contenido. */
	frame_length = strcspn(buffer, "\r\n");
	/* El tipo ocupa un caracter, despues debe venir | y un tema no vacio. */
	if (frame_length < 4 || buffer[1] != '|') {
		return -1;
	}
	separator = (const char *)memchr(buffer + 2, '|', frame_length - 2);
	if (separator == NULL) {
		return -1;
	}
	topic_length = (size_t)(separator - (buffer + 2));
	content = separator + 1;
	content_length = frame_length - (size_t)(content - buffer);
	/* Rechaza campos vacios o demasiado largos para los arreglos destino. */
	if (topic_length == 0 || topic_length >= TOPIC_MAX_LEN || content_length >= MSG_MAX_LEN) {
		return -1;
	}
	/* Verifica que el primer caracter corresponda a un tipo permitido. */
	switch (buffer[0]) {
		case PUBSUB_PUBLISH:
		case PUBSUB_SUBSCRIBE:
		case PUBSUB_MESSAGE:
			message->type = (PubSubMessageType)buffer[0];
			break;
		default:
			return -1;
	}
		/* Una suscripcion solo lleva tema; no puede traer contenido. */
	if (message->type == PUBSUB_SUBSCRIBE && content_length != 0) {
		return -1;
	}

	/* Copia los campos y agrega '\0' para convertirlos en cadenas C. */
	memcpy(message->topic, buffer + 2, topic_length);
	message->topic[topic_length] = '\0';
	memcpy(message->content, content, content_length);
	message->content[content_length] = '\0';
	return 0;
}

/* Evita incluir las mismas definiciones mas de una vez en una compilacion. */
#endif

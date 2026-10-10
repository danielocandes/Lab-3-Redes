// cosas que comparten tcp y udp: puertos, limites y el formato de los mensajes
#ifndef PUBSUB_COMMON_H
#define PUBSUB_COMMON_H

#include <stddef.h>
#include <stdio.h>
#include <string.h>

// puerto de cada broker (el 5000 lo usa airplay en mac)
#define PORT_TCP        5050
#define PORT_UDP        5001

// tamanos maximos para que ningun mensaje se pase del buffer
#define TOPIC_MAX_LEN   64
#define MSG_MAX_LEN     256
#define BUFFER_SIZE     (TOPIC_MAX_LEN + MSG_MAX_LEN + 16)

// tipos de mensaje: publicar, suscribirse y entregar
typedef enum {
	PUBSUB_PUBLISH = 'P',
	PUBSUB_SUBSCRIBE = 'S',
	PUBSUB_MESSAGE = 'M'
} PubSubMessageType;

// un mensaje ya separado en sus tres partes
typedef struct {
	PubSubMessageType type;
	char topic[TOPIC_MAX_LEN];
	char content[MSG_MAX_LEN];
} PubSubMessage;

// arma la noticia de ejemplo (sirve para cualquier tema), con #numero para ver si algo se pierde o llega desordenado
static inline int pubsub_format_sports_event(char *content, size_t content_size,
											 long sequence, long message_count) {
	static const char *football_events[] = {
		"[GOL] Minuto 12: Mateo Ruiz marca para los Tigres.",
		"[TARJETA AMARILLA] Minuto 24: amonestado Daniel Rojas.",
		"[CAMBIO] Minuto 38: sale Luis Torres y entra Andres Silva.",
		"[GOL] Minuto 45: los Leones empatan con remate de Carlos Diaz.",
		"[TARJETA AMARILLA] Minuto 53: amonestado Juan Perez.",
		"[TARJETA ROJA] Minuto 67: expulsado Daniel Rojas por doble amarilla.",
		"[CAMBIO] Minuto 74: sale Mateo Ruiz y entra Santiago Mora.",
		"[RESULTADO PARCIAL] Minuto 80: Tigres 1 - 1 Leones.",
		"[GOL] Minuto 88: Santiago Mora pone en ventaja a los Tigres."
	};
	int written;

	if (content == NULL || content_size == 0 || sequence < 1 || message_count < 1) {
		return -1;
	}
	if (sequence == message_count) {
		written = snprintf(content, content_size,
							 "#%ld [RESULTADO FINAL] Tigres 2 - 1 Leones. Partido terminado.",
							 sequence);
	} else {
		size_t event_count = sizeof(football_events) / sizeof(football_events[0]);
		written = snprintf(content, content_size, "#%ld %s", sequence,
							 football_events[(size_t)(sequence - 1) % event_count]);
	}

	return written < 0 || (size_t)written >= content_size ? -1 : written;
}

// de struct a texto TIPO|TEMA|CONTENIDO
static inline int pubsub_encode(const PubSubMessage *message, char *buffer,
								size_t buffer_size) {
	int length;

	if (message == NULL || buffer == NULL || buffer_size == 0 ||
		(message->type != PUBSUB_PUBLISH &&
		 message->type != PUBSUB_SUBSCRIBE &&
		 message->type != PUBSUB_MESSAGE)) {
		return -1;
	}
	// revisamos que nada rompa el formato
	if (memchr(message->topic, '\0', sizeof(message->topic)) == NULL ||
		memchr(message->content, '\0', sizeof(message->content)) == NULL ||
		message->topic[0] == '\0' ||
		strpbrk(message->topic, "|\r\n") != NULL ||
		strpbrk(message->content, "\r\n") != NULL ||
		(message->type == PUBSUB_SUBSCRIBE && message->content[0] != '\0')) {
		return -1;
	}
	length = snprintf(buffer, buffer_size, "%c|%s|%s", (char)message->type,
					  message->topic, message->content);
	if (length < 0 || (size_t)length >= buffer_size) {
		return -1;
	}

	return length;
}

// de texto a struct, devuelve -1 si el formato esta mal
static inline int pubsub_decode(const char *buffer, PubSubMessage *message) {
	const char *separator;
	const char *content;
	size_t topic_length;
	size_t content_length;
	size_t frame_length;

	if (buffer == NULL || message == NULL) {
		return -1;
	}
	frame_length = strcspn(buffer, "\r\n");
	if (frame_length < 4 || buffer[1] != '|') {
		return -1;
	}
	// buscamos el segundo | que separa tema y contenido
	separator = (const char *)memchr(buffer + 2, '|', frame_length - 2);
	if (separator == NULL) {
		return -1;
	}
	topic_length = (size_t)(separator - (buffer + 2));
	content = separator + 1;
	content_length = frame_length - (size_t)(content - buffer);
	if (topic_length == 0 || topic_length >= TOPIC_MAX_LEN || content_length >= MSG_MAX_LEN) {
		return -1;
	}
	switch (buffer[0]) {
		case PUBSUB_PUBLISH:
		case PUBSUB_SUBSCRIBE:
		case PUBSUB_MESSAGE:
			message->type = (PubSubMessageType)buffer[0];
			break;
		default:
			return -1;
	}
	if (message->type == PUBSUB_SUBSCRIBE && content_length != 0) {
		return -1;
	}

	// copiamos los campos y los volvemos strings de C
	memcpy(message->topic, buffer + 2, topic_length);
	message->topic[topic_length] = '\0';
	memcpy(message->content, content, content_length);
	message->content[content_length] = '\0';
	return 0;
}

#endif

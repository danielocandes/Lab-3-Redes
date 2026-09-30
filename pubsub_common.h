#ifndef PUBSUB_COMMON_H
#define PUBSUB_COMMON_H

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#define PORT_TCP        5000
#define PORT_UDP        5001

#define TOPIC_MAX_LEN   64
#define MSG_MAX_LEN     256
#define BUFFER_SIZE     (TOPIC_MAX_LEN + MSG_MAX_LEN + 16)

typedef enum {
	PUBSUB_PUBLISH = 'P',
	PUBSUB_SUBSCRIBE = 'S',
	PUBSUB_MESSAGE = 'M'
} PubSubMessageType;

typedef struct {
	PubSubMessageType type;
	char topic[TOPIC_MAX_LEN];
	char content[MSG_MAX_LEN];
} PubSubMessage;

// Convierte un mensaje a texto y comprueba que quepa en el buffer.
static inline int pubsub_encode(const PubSubMessage *message, char *buffer,
								size_t buffer_size) {
	int length;

	if (message == NULL || buffer == NULL || buffer_size == 0 ||
		(message->type != PUBSUB_PUBLISH &&
		 message->type != PUBSUB_SUBSCRIBE &&
		 message->type != PUBSUB_MESSAGE)) {
		return -1;
	}
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

// Separa tipo, tema y contenido; devuelve -1 si el formato no es valido.
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

	memcpy(message->topic, buffer + 2, topic_length);
	message->topic[topic_length] = '\0';
	memcpy(message->content, content, content_length);
	message->content[content_length] = '\0';
	return 0;
}

#endif

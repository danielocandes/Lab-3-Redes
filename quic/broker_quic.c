/* Habilita interfaces POSIX para usar IPv4 y pause() en Linux/Unix. */
#define _POSIX_C_SOURCE 200809L

/* Direcciones, mutex, MsQuic, memoria, texto y espera del proceso. */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <msquic.h>

#include "../pubsub_common.h"

#define QUIC_ALPN "lab3-pubsub"

/* Declaraciones adelantadas: Broker y StreamContext contienen punteros mutuos. */
typedef struct Broker Broker;
typedef struct StreamContext StreamContext;

/* Entrada de la lista: un tema y el stream QUIC que pidio recibirlo. */
typedef struct Subscription {
	StreamContext *stream_context;
	char topic[TOPIC_MAX_LEN];
	struct Subscription *next;
} Subscription;

/* Recursos del servidor y coleccion de suscripciones compartida. */
struct Broker {
	const QUIC_API_TABLE *api;
	HQUIC registration;
	HQUIC configuration;
	HQUIC listener;
	/* MsQuic puede ejecutar callbacks a la vez; el mutex protege la lista. */
	pthread_mutex_t mutex;
	Subscription *subscriptions;
};

/* Estado de una conexion de aplicacion, independiente para cada stream. */
struct StreamContext {
	Broker *broker;
	HQUIC stream;
	/* Acumula bytes recibidos hasta newline, que delimita cada mensaje. */
	char input[BUFFER_SIZE];
	size_t used;
	int is_subscriber;
};

/* La memoria enviada se conserva hasta que MsQuic avise SEND_COMPLETE. */
typedef struct {
	QUIC_BUFFER buffer;
	char bytes[BUFFER_SIZE];
} BrokerSendBuffer;

/* Agrega la suscripcion solo si ese stream aun no tenia ese tema. */
static int add_subscription(StreamContext *stream_context, const char *topic) {
	Broker *broker = stream_context->broker;
	Subscription *item;
	/* Inicio de la seccion critica: callbacks pueden acceder a la lista juntos. */
	pthread_mutex_lock(&broker->mutex);
	for (item = broker->subscriptions; item != NULL; item = item->next) {
		if (item->stream_context == stream_context && strcmp(item->topic, topic) == 0) {
			pthread_mutex_unlock(&broker->mutex);
			return 0;
		}
	}
	/* Reserva una entrada que seguira viva mientras el subscriber este conectado. */
	item = malloc(sizeof(*item));
	if (item == NULL) {
		pthread_mutex_unlock(&broker->mutex);
		return -1;
	}
	item->stream_context = stream_context;
	memcpy(item->topic, topic, strlen(topic) + 1);
	item->next = broker->subscriptions;
	broker->subscriptions = item;
	/* Marca el stream para distinguir sus mensajes S de publicaciones P. */
	stream_context->is_subscriber = 1;
	pthread_mutex_unlock(&broker->mutex);
	printf("QUIC subscriber registered for %s\n", topic);
	fflush(stdout);
	return 0;
}

/* Elimina todos los temas asociados a un stream que acaba de cerrar. */
static void remove_stream_subscriptions(StreamContext *stream_context) {
	Broker *broker = stream_context->broker;
	Subscription **current;
	pthread_mutex_lock(&broker->mutex);
	current = &broker->subscriptions;
	/* Recorre enlaces; al borrar, mantiene la lista enlazada consistente. */
	while (*current != NULL) {
		if ((*current)->stream_context == stream_context) {
			Subscription *removed = *current;
			*current = removed->next;
			free(removed);
		} else {
			current = &(*current)->next;
		}
	}
	pthread_mutex_unlock(&broker->mutex);
}

/* Convierte P en M y manda una copia a cada subscriber del tema. */
static void publish_message(Broker *broker, const PubSubMessage *published) {
	PubSubMessage delivered = *published;
	Subscription *item;
	char frame[BUFFER_SIZE];
	int length;
	/* No modifica la estructura original que envio el publisher. */
	delivered.type = PUBSUB_MESSAGE;
	length = pubsub_encode(&delivered, frame, sizeof(frame));
	if (length < 0 || (size_t)length + 1 >= sizeof(frame)) return;
	/* Newline separa mensajes dentro del stream de bytes QUIC. */
	frame[length++] = '\n';
	/* Mantiene estable la lista mientras se encolan las entregas. */
	pthread_mutex_lock(&broker->mutex);
	for (item = broker->subscriptions; item != NULL; item = item->next) {
		BrokerSendBuffer *send_buffer;
		QUIC_STATUS status;
		if (strcmp(item->topic, delivered.topic) != 0) continue;
		/* Cada envio necesita una copia propia hasta el callback SEND_COMPLETE. */
		send_buffer = malloc(sizeof(*send_buffer));
		if (send_buffer == NULL) continue;
		memcpy(send_buffer->bytes, frame, (size_t)length);
		send_buffer->buffer.Length = (uint32_t)length;
		send_buffer->buffer.Buffer = (uint8_t *)send_buffer->bytes;
		status = broker->api->StreamSend(item->stream_context->stream,
										 &send_buffer->buffer, 1, QUIC_SEND_FLAG_NONE,
										 send_buffer);
		if (QUIC_FAILED(status)) free(send_buffer);
	}
	pthread_mutex_unlock(&broker->mutex);
}

/* Decodifica una linea y decide si registrar o distribuir el mensaje. */
static void process_frame(StreamContext *stream_context) {
	PubSubMessage message;
	if (pubsub_decode(stream_context->input, &message) != 0) return;
	if (message.type == PUBSUB_SUBSCRIBE) {
		add_subscription(stream_context, message.topic);
	} else if (message.type == PUBSUB_PUBLISH && !stream_context->is_subscriber) {
		publish_message(stream_context->broker, &message);
	}
}

/* MsQuic invoca este callback cuando llegan datos o termina un stream. */
static QUIC_STATUS QUIC_API stream_callback(HQUIC stream, void *context,
											QUIC_STREAM_EVENT *event) {
	StreamContext *stream_context = context;
	if (event->Type == QUIC_STREAM_EVENT_RECEIVE) {
		/* Un evento puede contener varios buffers y cada uno varios bytes. */
		for (uint32_t buffer_index = 0; buffer_index < event->RECEIVE.BufferCount;
			 buffer_index++) {
			QUIC_BUFFER *buffer = &event->RECEIVE.Buffers[buffer_index];
			for (uint32_t byte_index = 0; byte_index < buffer->Length; byte_index++) {
				char byte = (char)buffer->Buffer[byte_index];
				/* Evita sobrepasar el tamano maximo si la entrada no termina. */
				if (stream_context->used + 1 >= sizeof(stream_context->input)) {
					stream_context->used = 0;
					continue;
				}
				stream_context->input[stream_context->used++] = byte;
				/* Una linea completa se procesa y el acumulador queda listo para otra. */
				if (byte == '\n') {
					stream_context->input[stream_context->used] = '\0';
					process_frame(stream_context);
					stream_context->used = 0;
				}
			}
		}
		/* FIN es el cierre de envio del publisher; se devuelve FIN al cerrar. */
		if ((event->RECEIVE.Flags & QUIC_RECEIVE_FLAG_FIN) != 0 &&
			!stream_context->is_subscriber) {
			stream_context->broker->api->StreamShutdown(
				stream, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0);
		}
	} else if (event->Type == QUIC_STREAM_EVENT_SEND_COMPLETE) {
		/* La biblioteca ya no necesita la copia usada en la entrega. */
		free(event->SEND_COMPLETE.ClientContext);
	} else if (event->Type == QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE) {
		/* Quita temas y libera memoria/handle al terminar completamente el stream. */
		remove_stream_subscriptions(stream_context);
		stream_context->broker->api->StreamClose(stream);
		free(stream_context);
	}
	return QUIC_STATUS_SUCCESS;
}

/* Callback por conexion: recibe streams nuevos que abren los clientes. */
static QUIC_STATUS QUIC_API connection_callback(HQUIC connection, void *context,
												QUIC_CONNECTION_EVENT *event) {
	Broker *broker = context;
	if (event->Type == QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED) {
		/* MsQuic avisa que publisher/subscriber abrio un canal de datos. */
		StreamContext *stream_context = calloc(1, sizeof(*stream_context));
		if (stream_context == NULL) return QUIC_STATUS_OUT_OF_MEMORY;
		stream_context->broker = broker;
		stream_context->stream = event->PEER_STREAM_STARTED.Stream;
		/* Asocia el canal con stream_callback y con su propio contexto. */
		broker->api->SetCallbackHandler(stream_context->stream,
												 (void *)stream_callback, stream_context);
	} else if (event->Type == QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE) {
		/* Cuando toda la conexion termino, libera el handle de MsQuic. */
		broker->api->ConnectionClose(connection);
	}
	return QUIC_STATUS_SUCCESS;
}

/* Callback del listener: acepta una conexion QUIC entrante y le configura TLS. */
static QUIC_STATUS QUIC_API listener_callback(HQUIC listener, void *context,
												QUIC_LISTENER_EVENT *event) {
	Broker *broker = context;
	(void)listener;
	if (event->Type == QUIC_LISTENER_EVENT_NEW_CONNECTION) {
		/* Instala callback de conexion y credenciales para completar handshake. */
		HQUIC connection = event->NEW_CONNECTION.Connection;
		broker->api->SetCallbackHandler(connection, (void *)connection_callback, broker);
		return broker->api->ConnectionSetConfiguration(connection, broker->configuration);
	}
	return QUIC_STATUS_SUCCESS;
}

/* Carga certificado, abre el listener y espera clientes QUIC. */
int main(int argc, char **argv) {
	Broker broker = {0};
	/* MsQuic registra el nombre de esta aplicacion y su perfil de trabajo. */
	QUIC_REGISTRATION_CONFIG registration_config = {
		"lab3-pubsub-broker", QUIC_EXECUTION_PROFILE_LOW_LATENCY
	};
	QUIC_BUFFER alpn = { (uint32_t)(sizeof(QUIC_ALPN) - 1), (uint8_t *)QUIC_ALPN };
	QUIC_SETTINGS settings = {0};
	QUIC_CERTIFICATE_FILE certificate_file;
	QUIC_CREDENTIAL_CONFIG credentials = {0};
	QUIC_ADDR address = {0};
	QUIC_STATUS status;
	long port = PORT_QUIC;
	char *end;

	/* El servidor necesita un certificado TLS y su llave privada. */
	if (argc < 3 || argc > 4) {
		fprintf(stderr, "Usage: %s <certificate.pem> <private-key.pem> [port]\n", argv[0]);
		return EXIT_FAILURE;
	}
	if (argc == 4) {
		port = strtol(argv[3], &end, 10);
		if (*argv[3] == '\0' || *end != '\0' || port < 1 || port > 65535) {
			fprintf(stderr, "port must be between 1 and 65535\n");
			return EXIT_FAILURE;
		}
	}
	if (pthread_mutex_init(&broker.mutex, NULL) != 0) return EXIT_FAILURE;
	/* Abre la biblioteca MsQuic y crea los recursos principales del servidor. */
	status = MsQuicOpen2(&broker.api);
	if (QUIC_FAILED(status)) goto fail;
	status = broker.api->RegistrationOpen(&registration_config, &broker.registration);
	if (QUIC_FAILED(status)) goto fail;
	/* Permite que los clientes abran streams bidireccionales en cada conexion. */
	settings.PeerBidiStreamCount = 16;
	settings.IsSet.PeerBidiStreamCount = TRUE;
	status = broker.api->ConfigurationOpen(broker.registration, &alpn, 1, &settings,
											 sizeof(settings), NULL, &broker.configuration);
	if (QUIC_FAILED(status)) goto fail;
	/* Configura los archivos PEM que se usaran durante el handshake TLS. */
	certificate_file.CertificateFile = argv[1];
	certificate_file.PrivateKeyFile = argv[2];
	credentials.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
	credentials.CertificateFile = &certificate_file;
	status = broker.api->ConfigurationLoadCredential(broker.configuration, &credentials);
	if (QUIC_FAILED(status)) goto fail;
	status = broker.api->ListenerOpen(broker.registration, listener_callback, &broker,
											 &broker.listener);
	if (QUIC_FAILED(status)) goto fail;
	/* Escucha en todas las interfaces IPv4 usando el puerto QUIC elegido. */
	address.Ipv4.sin_family = AF_INET;
	address.Ipv4.sin_addr.s_addr = htonl(INADDR_ANY);
	address.Ipv4.sin_port = htons((uint16_t)port);
	status = broker.api->ListenerStart(broker.listener, &alpn, 1, &address);
	if (QUIC_FAILED(status)) goto fail;
	printf("QUIC broker listening on UDP port %ld\n", port);
	fflush(stdout);
	/* Los workers de MsQuic procesan red/callbacks; aqui el hilo principal espera. */
	for (;;) pause();

fail:
	/* Informa por que MsQuic no pudo iniciar listener o cargar TLS. */
	fprintf(stderr, "QUIC broker setup failed (0x%x)\n", (unsigned int)status);
	return EXIT_FAILURE;
}
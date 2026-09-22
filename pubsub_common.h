/*
 * pubsub_common.h
 * Constantes y formato de mensaje compartidos entre broker/publisher/subscriber,
 * tanto en la version TCP como en la UDP.
 *
 * Referencia: Laboratorio 3 - Analisis Capa de Transporte y Sockets (ISIS-2311).
 */

#ifndef PUBSUB_COMMON_H
#define PUBSUB_COMMON_H

#define PORT_TCP        5000
#define PORT_UDP        5001

#define TOPIC_MAX_LEN   64      /* nombre del partido, ej. "EquipoA_vs_EquipoB" */
#define MSG_MAX_LEN     256     /* ej. "Gol de Equipo A al minuto 32" */
#define BUFFER_SIZE     (TOPIC_MAX_LEN + MSG_MAX_LEN + 16)

/*
 * TODO: definir aca el formato de trama que broker/publisher/subscriber van a
 * compartir (ej. un char de tipo + topico + mensaje, separados por delimitador,
 * o un struct empaquetado). Debe alcanzar para que el broker distinga:
 *   - un publisher publicando en un topico
 *   - un subscriber suscribiendose a un topico
 *   - el mensaje reenviado a los subscribers
 */

#endif /* PUBSUB_COMMON_H */

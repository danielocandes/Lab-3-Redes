#ifndef PUBSUB_COMMON_H
#define PUBSUB_COMMON_H

#define PORT_TCP        5000
#define PORT_UDP        5001

#define TOPIC_MAX_LEN   64
#define MSG_MAX_LEN     256
#define BUFFER_SIZE     (TOPIC_MAX_LEN + MSG_MAX_LEN + 16)

#endif

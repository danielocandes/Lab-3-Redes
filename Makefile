CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -pthread

TCP_BINS = tcp/broker_tcp tcp/publisher_tcp tcp/subscriber_tcp
UDP_BINS = udp/broker_udp udp/publisher_udp udp/subscriber_udp

all: $(TCP_BINS) $(UDP_BINS)

# cada binario sale de su .c
$(TCP_BINS) $(UDP_BINS): %: %.c pubsub_common.h
	$(CC) $(CFLAGS) -o $@ $<

tcp: $(TCP_BINS)
udp: $(UDP_BINS)

clean:
	rm -f $(TCP_BINS) $(UDP_BINS)

.PHONY: all tcp udp clean

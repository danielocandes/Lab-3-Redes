CC = gcc
CFLAGS = -Wall -Wextra -std=c11

TCP_BINS = broker_tcp publisher_tcp subscriber_tcp
UDP_BINS = broker_udp publisher_udp subscriber_udp
BINS = $(TCP_BINS) $(UDP_BINS)

all: $(BINS)

%: %.c pubsub_common.h
	$(CC) $(CFLAGS) -o $@ $<

tcp: $(TCP_BINS)

udp: $(UDP_BINS)

clean:
	rm -f $(BINS)

.PHONY: all tcp udp clean

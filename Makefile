# Compilador C usado para crear los ejecutables.
CC = gcc
# Opciones: advertencias, estandar C11 y soporte POSIX para hilos.
CFLAGS = -Wall -Wextra -std=c11 -pthread

# Cada lista contiene los tres programas que forman un transporte.
TCP_BINS = tcp/broker_tcp tcp/publisher_tcp tcp/subscriber_tcp
UDP_BINS = udp/broker_udp udp/publisher_udp udp/subscriber_udp
# QUIC depende de MsQuic; por eso no se agrega al objetivo all.
BINS = $(TCP_BINS) $(UDP_BINS) quic/broker_quic quic/publisher_quic quic/subscriber_quic

# make (sin argumento) compila solo los transportes obligatorios.
all: $(TCP_BINS) $(UDP_BINS)

# Regla generica: cada binario TCP se obtiene de su archivo .c correspondiente.
$(TCP_BINS): %: %.c pubsub_common.h
	$(CC) $(CFLAGS) -o $@ $<

# La misma regla generica para los binarios UDP.
$(UDP_BINS): %: %.c pubsub_common.h
	$(CC) $(CFLAGS) -o $@ $<

# Atajos para compilar una familia completa.
tcp: $(TCP_BINS)

udp: $(UDP_BINS)

quic: quic/broker_quic quic/publisher_quic quic/subscriber_quic

# La regla QUIC enlaza (-l) la biblioteca externa MsQuic.
quic/%: quic/%.c quic/quic_client_common.h pubsub_common.h
	$(CC) $(CFLAGS) -o $@ $< -lmsquic

# Elimina solo binarios compilados, no fuentes ni datos del proyecto.
clean:
	rm -f $(BINS)

# Declara nombres de tareas que no corresponden a archivos reales.
.PHONY: all tcp udp quic clean

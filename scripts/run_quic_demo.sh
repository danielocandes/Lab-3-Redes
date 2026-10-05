#!/usr/bin/env bash
# Detiene la prueba si falla la compilacion, el arranque o una comprobacion.
set -eu

# Encuentra la raiz del proyecto sin importar la carpeta desde la que se ejecuta.
root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"
make quic

# Certificado y logs temporales: no se agregan al repositorio.
temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/pubsub-quic.XXXXXX")
broker_pid=
subscriber_a_pid=
subscriber_b_pid=
publisher_a_pid=
publisher_b_pid=
cleanup() {
	# Detiene unicamente procesos iniciados por esta prueba y borra sus archivos.
	for process_id in "$publisher_a_pid" "$publisher_b_pid" \
		"$subscriber_a_pid" "$subscriber_b_pid" "$broker_pid"; do
		if [ -n "$process_id" ]; then
			kill "$process_id" 2>/dev/null || true
			wait "$process_id" 2>/dev/null || true
		fi
	done
	rm -rf "$temp_dir"
}
# Garantiza limpieza incluso si la prueba termina con error.
trap cleanup EXIT

# QUIC siempre usa TLS: el certificado efimero permite probar localmente.
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$temp_dir/key.pem" \
	-out "$temp_dir/cert.pem" -days 1 -subj "/CN=localhost" >/dev/null 2>&1
./quic/broker_quic "$temp_dir/cert.pem" "$temp_dir/key.pem" \
	>"$temp_dir/broker.log" 2>&1 &
# Guarda el PID del broker para detenerlo en cleanup.
broker_pid=$!
sleep 0.3
./quic/subscriber_quic 127.0.0.1 demo_topic_a >"$temp_dir/sub-a.log" 2>&1 &
subscriber_a_pid=$!
./quic/subscriber_quic 127.0.0.1 demo_topic_b >"$temp_dir/sub-b.log" 2>&1 &
subscriber_b_pid=$!

# Espera que ambos subscribers queden registrados antes de publicar.
for attempt in $(seq 1 30); do
	if grep -q 'registered for demo_topic_a' "$temp_dir/broker.log" && \
		grep -q 'registered for demo_topic_b' "$temp_dir/broker.log"; then
		break
	fi
	sleep 0.1
done

# Publica en paralelo diez mensajes para cada tema.
./quic/publisher_quic 127.0.0.1 demo_topic_a 10 &
publisher_a_pid=$!
./quic/publisher_quic 127.0.0.1 demo_topic_b 10 &
publisher_b_pid=$!
wait "$publisher_a_pid"
wait "$publisher_b_pid"
sleep 0.5

# Cuenta mensajes correctos y mensajes del tema incorrecto en cada log.
count_a=$(grep -cF '[demo_topic_a]' "$temp_dir/sub-a.log" || true)
count_b=$(grep -cF '[demo_topic_b]' "$temp_dir/sub-b.log" || true)
wrong_a=$(grep -cF '[demo_topic_b]' "$temp_dir/sub-a.log" || true)
wrong_b=$(grep -cF '[demo_topic_a]' "$temp_dir/sub-b.log" || true)
if [ "$count_a" -ne 10 ] || [ "$count_b" -ne 10 ] || \
	[ "$wrong_a" -ne 0 ] || [ "$wrong_b" -ne 0 ]; then
	printf 'FAIL: topic delivery A=%s B=%s wrong A=%s B=%s\n' \
		"$count_a" "$count_b" "$wrong_a" "$wrong_b" >&2
	cat "$temp_dir/broker.log" "$temp_dir/sub-a.log" "$temp_dir/sub-b.log" >&2
	exit 1
fi
printf 'PASS: QUIC delivered 10 messages per topic with topic filtering\n'
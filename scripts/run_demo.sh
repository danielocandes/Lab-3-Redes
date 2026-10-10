#!/usr/bin/env bash
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    printf 'Usage: %s tcp|udp [udp-first-message-delay-ms]\n' "$0" >&2
    exit 2
fi

mode=$1
delay_ms=${2:-500}
if [ "$mode" != tcp ] && [ "$mode" != udp ]; then
    printf 'Mode must be tcp or udp\n' >&2
    exit 2
fi
if [ "$mode" = udp ]; then
    case $delay_ms in
        ''|*[!0-9]*)
            printf 'UDP delay must be a positive integer in milliseconds\n' >&2
            exit 2
            ;;
    esac
    if [ "$delay_ms" -eq 0 ]; then
        printf 'UDP delay must be greater than zero for the reorder check\n' >&2
        exit 2
    fi
fi

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"
make "$mode"

temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/pubsub-demo.XXXXXX")
broker_pid=
subscriber_a_pid=
subscriber_b_pid=
publisher_a_pid=
publisher_b_pid=

cleanup() {
    for process_id in "$publisher_a_pid" "$publisher_b_pid" \
        "$subscriber_a_pid" "$subscriber_b_pid" "$broker_pid"; do
        if [ -n "$process_id" ]; then
            kill "$process_id" 2>/dev/null || true
            wait "$process_id" 2>/dev/null || true
        fi
    done
    rm -rf "$temp_dir"
}
# al salir mata los procesos y borra los logs
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# cada subscriber escucha un tema distinto para ver que el broker filtra
topic_a=demo_topic_a
topic_b=demo_topic_b
if [ "$mode" = tcp ]; then
    ./tcp/broker_tcp >"$temp_dir/broker.log" 2>&1 &
else
    ./udp/broker_udp "$delay_ms" >"$temp_dir/broker.log" 2>&1 &
fi
broker_pid=$!
sleep 0.2

if [ "$mode" = tcp ]; then
    ./tcp/subscriber_tcp 127.0.0.1 "$topic_a" >"$temp_dir/subscriber-a.log" 2>&1 &
    subscriber_a_pid=$!
    ./tcp/subscriber_tcp 127.0.0.1 "$topic_b" >"$temp_dir/subscriber-b.log" 2>&1 &
    subscriber_b_pid=$!
else
    ./udp/subscriber_udp 127.0.0.1 "$topic_a" >"$temp_dir/subscriber-a.log" 2>&1 &
    subscriber_a_pid=$!
    ./udp/subscriber_udp 127.0.0.1 "$topic_b" >"$temp_dir/subscriber-b.log" 2>&1 &
    subscriber_b_pid=$!
fi

sleep 0.2

if [ "$mode" = tcp ]; then
    ./tcp/publisher_tcp 127.0.0.1 "$topic_a" 10 &
    publisher_a_pid=$!
    ./tcp/publisher_tcp 127.0.0.1 "$topic_b" 10 &
    publisher_b_pid=$!
else
    ./udp/publisher_udp 127.0.0.1 "$topic_a" 10 &
    publisher_a_pid=$!
    ./udp/publisher_udp 127.0.0.1 "$topic_b" 10 &
    publisher_b_pid=$!
fi
# esperamos a que los publishers terminen
wait "$publisher_a_pid"
wait "$publisher_b_pid"

wait_seconds=1
if [ "$mode" = udp ]; then
    wait_seconds=$((delay_ms / 1000 + 1))
fi
sleep "$wait_seconds"

# tienen que llegar los 10 mensajes de su tema y ninguno del otro
count_a=$(grep -cF "[$topic_a]" "$temp_dir/subscriber-a.log" || true)
count_b=$(grep -cF "[$topic_b]" "$temp_dir/subscriber-b.log" || true)
if [ "$count_a" -ne 10 ] || [ "$count_b" -ne 10 ]; then
    printf 'Expected 10 messages per topic; received A=%s B=%s\n' "$count_a" "$count_b" >&2
    cat "$temp_dir/subscriber-a.log" "$temp_dir/subscriber-b.log" >&2
    exit 1
fi

other_topic_count=$(grep -cF "[$topic_b]" "$temp_dir/subscriber-a.log" || true)
if [ "$other_topic_count" -ne 0 ]; then
    printf 'Subscriber A received messages for the wrong topic\n' >&2
    exit 1
fi
other_topic_count=$(grep -cF "[$topic_a]" "$temp_dir/subscriber-b.log" || true)
if [ "$other_topic_count" -ne 0 ]; then
    printf 'Subscriber B received messages for the wrong topic\n' >&2
    exit 1
fi

printf 'PASS: %s delivered 10 messages per topic with topic filtering\n' "$mode"
if [ "$mode" = udp ]; then
    # el mensaje 1 tiene que llegar de ultimo
    reordered=0
    for log_file in "$temp_dir/subscriber-a.log" "$temp_dir/subscriber-b.log"; do
        first_sequence=$(sed -n 's/^\[[^]]*\] #\([0-9][0-9]*\) .*/\1/p' "$log_file" | head -n 1)
        last_sequence=$(sed -n 's/^\[[^]]*\] #\([0-9][0-9]*\) .*/\1/p' "$log_file" | tail -n 1)
        if [ "$first_sequence" != 1 ] && [ "$last_sequence" = 1 ]; then
            reordered=1
        fi
    done
    if [ "$reordered" -ne 1 ]; then
        printf 'Expected one delayed UDP message to arrive after messages 2-10\n' >&2
        cat "$temp_dir/subscriber-a.log" "$temp_dir/subscriber-b.log" >&2
        exit 1
    fi
    printf 'PASS: delayed first UDP message arrived last\n'
fi
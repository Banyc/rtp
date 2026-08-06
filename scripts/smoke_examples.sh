#!/bin/sh
set -eu

cargo build --examples -j1
work=$(mktemp -d)
server_pid=
cleanup() {
    if [ -n "$server_pid" ]; then kill "$server_pid" 2>/dev/null || true; fi
    rm -rf "$work"
}
trap cleanup EXIT INT TERM

dd if=/dev/urandom of="$work/source" bs=1048576 count=1 2>/dev/null

run_case() {
    scheme=$1
    port=$2
    fec=$3
    received="$work/received-$scheme-$port"
    if [ "$fec" = yes ]; then
        ./target/debug/examples/server --fec "$scheme://127.0.0.1:$port" \
            pull "$received" >"$work/server-$port.log" 2>&1 &
    else
        ./target/debug/examples/server "$scheme://127.0.0.1:$port" \
            pull "$received" >"$work/server-$port.log" 2>&1 &
    fi
    server_pid=$!
    sleep 0.25
    if [ "$fec" = yes ]; then
        ./target/debug/examples/client --fec "$scheme://127.0.0.1:$port" \
            push "$work/source"
    else
        ./target/debug/examples/client "$scheme://127.0.0.1:$port" \
            push "$work/source"
    fi
    wait "$server_pid"
    server_pid=
    cmp "$work/source" "$received"
}

run_case rtp 42112 no
run_case rtp 42113 yes
run_case rtpm 42114 no

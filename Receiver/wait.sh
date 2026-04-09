#!/bin/sh

host="$1"
port="$2"
shift 2
cmd="$@"

echo "Waiting for $host:$port..."

while ! timeout 1 bash -c "</dev/tcp/$host/$port" 2>/dev/null; do
  sleep 1
done

echo "$host:$port is up - starting app"

exec $cmd
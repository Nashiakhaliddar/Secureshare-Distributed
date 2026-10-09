#!/usr/bin/env bash
# Run the complete distributed system WITHOUT Docker (Linux / macOS / WSL).
#
#   ./scripts/local_cluster.sh start            start 5 storage nodes + 3 API nodes (+ nginx if installed)
#   ./scripts/local_cluster.sh status           show cluster status (JSON via the load balancer)
#   ./scripts/local_cluster.sh stop             stop everything
#   ./scripts/local_cluster.sh kill-storage s2  simulate a crash of storage node s2
#   ./scripts/local_cluster.sh start-storage s2 bring it back
#   ./scripts/local_cluster.sh kill-api 1       crash API node api-1 (this may be the leader!)
#   ./scripts/local_cluster.sh start-api 1      bring it back
#   ./scripts/local_cluster.sh logs api-1       tail a node's log
#
# Requirements: PostgreSQL running locally, `cargo` (Rust), optionally `nginx`.
# Override the DB with:  DATABASE_URL=postgres://user:pass@host:5432/db ./scripts/local_cluster.sh start

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="$ROOT/.run"
BIN="$ROOT/backend/target/release"
mkdir -p "$RUN/pids" "$RUN/logs" "$RUN/data"

DATABASE_URL="${DATABASE_URL:-postgres://secureshare:secureshare@localhost:5432/secureshare}"
STORAGE_IDS=(s1 s2 s3 s4 s5)
API_COUNT=3
LB_PORT="${LB_PORT:-8080}"

# ---- secrets are generated once and kept in .run/secrets.env
if [ ! -f "$RUN/secrets.env" ]; then
  {
    echo "MASTER_KEY=$(openssl rand -hex 32)"
    echo "JWT_SECRET_KEY=$(openssl rand -hex 24)"
    echo "CLUSTER_TOKEN=$(openssl rand -hex 16)"
  } > "$RUN/secrets.env"
fi
# shellcheck disable=SC1091
source "$RUN/secrets.env"

storage_port() { echo $((9000 + ${1#s})); }
api_port()     { echo $((8000 + $1)); }

storage_nodes_env() {
  local out=""
  for id in "${STORAGE_IDS[@]}"; do out+="${id}=http://127.0.0.1:$(storage_port "$id"),"; done
  echo "${out%,}"
}

ensure_built() {
  if [ ! -x "$BIN/secureshare_backend" ] || [ ! -x "$BIN/storage_node" ]; then
    echo "building backend (first time takes a few minutes)..."
    (cd "$ROOT/backend" && cargo build --release)
  fi
}

# run in a new session so the nodes survive when this terminal/script exits
if command -v setsid >/dev/null 2>&1; then DETACH="setsid"; else DETACH=""; fi

is_running() { [ -f "$RUN/pids/$1.pid" ] && kill -0 "$(cat "$RUN/pids/$1.pid")" 2>/dev/null; }

start_storage() {
  local id="$1"
  if is_running "$id"; then echo "$id already running"; return; fi
  NODE_ID="$id" PORT="$(storage_port "$id")" DATA_DIR="$RUN/data/$id" CLUSTER_TOKEN="$CLUSTER_TOKEN" \
    $DETACH nohup "$BIN/storage_node" >> "$RUN/logs/$id.log" 2>&1 < /dev/null &
  echo $! > "$RUN/pids/$id.pid"
  echo "started storage node $id on :$(storage_port "$id")"
}

start_api() {
  local n="$1" id="api-$1"
  if is_running "$id"; then echo "$id already running"; return; fi
  NODE_ID="$id" PORT="$(api_port "$n")" DATABASE_URL="$DATABASE_URL" \
  JWT_SECRET_KEY="$JWT_SECRET_KEY" JWT_MAXAGE="${JWT_MAXAGE:-60}" MASTER_KEY="$MASTER_KEY" \
  CLUSTER_TOKEN="$CLUSTER_TOKEN" STORAGE_NODES="$(storage_nodes_env)" \
  CORS_ORIGIN="${CORS_ORIGIN:-http://localhost:3000}" \
  REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}" WRITE_QUORUM="${WRITE_QUORUM:-2}" \
  CHUNK_SIZE="${CHUNK_SIZE:-1048576}" PARALLELISM="${PARALLELISM:-8}" \
  REPAIR_AFTER_SECS="${REPAIR_AFTER_SECS:-10}" REPAIR_INTERVAL_SECS="${REPAIR_INTERVAL_SECS:-5}" \
  RUST_LOG="${RUST_LOG:-info,sqlx=warn,tower_http=warn}" \
    $DETACH nohup "$BIN/secureshare_backend" >> "$RUN/logs/$id.log" 2>&1 < /dev/null &
  echo $! > "$RUN/pids/$id.pid"
  echo "started API node $id on :$(api_port "$n")"
}

write_nginx_conf() {
  mkdir -p "$RUN/nginx_tmp"
  {
    echo "worker_processes 2;"
    echo "pid $RUN/nginx.pid;"
    echo "error_log $RUN/logs/nginx-error.log;"
    echo "events { worker_connections 1024; }"
    echo "http {"
    echo "  access_log off;"
    for d in client_body proxy fastcgi uwsgi scgi; do echo "  ${d}_temp_path $RUN/nginx_tmp/$d;"; done
    echo "  upstream api_cluster {"
    echo "    least_conn;"
    for n in $(seq 1 $API_COUNT); do echo "    server 127.0.0.1:$(api_port "$n") max_fails=2 fail_timeout=10s;"; done
    echo "  }"
    echo "  server {"
    echo "    listen $LB_PORT;"
    echo "    client_max_body_size 300m;"
    echo "    location /api/ {"
    echo "      proxy_pass http://api_cluster;"
    echo "      proxy_http_version 1.1;"
    echo "      proxy_set_header Connection \"\";"
    echo "      proxy_set_header Host \$host;"
    echo "      proxy_next_upstream error timeout http_502 http_503;"
    echo "      proxy_connect_timeout 3s;"
    echo "      proxy_read_timeout 300s;"
    echo "      proxy_buffering off;"
    echo "    }"
    echo "  }"
    echo "}"
  } > "$RUN/nginx.conf"
}

start_lb() {
  if command -v nginx >/dev/null 2>&1; then
    write_nginx_conf
    mkdir -p "$RUN/nginx_tmp"
    if [ -f "$RUN/nginx.pid" ] && kill -0 "$(cat "$RUN/nginx.pid")" 2>/dev/null; then
      echo "nginx already running"
    else
      nginx -c "$RUN/nginx.conf" -p "$RUN"
      echo "load balancer (nginx) on :$LB_PORT  ->  API nodes :$(api_port 1)-$(api_port $API_COUNT)"
    fi
  else
    echo "nginx not found: no load balancer. Point the frontend at one node, e.g."
    echo "  NEXT_PUBLIC_API_URL=http://localhost:$(api_port 1)/api"
  fi
}

stop_proc() {
  local name="$1"
  if is_running "$name"; then
    kill "$(cat "$RUN/pids/$name.pid")" 2>/dev/null || true
    echo "stopped $name"
  fi
  rm -f "$RUN/pids/$name.pid"
}

cmd="${1:-help}"
case "$cmd" in
  start)
    ensure_built
    for id in "${STORAGE_IDS[@]}"; do start_storage "$id"; done
    sleep 1
    for n in $(seq 1 $API_COUNT); do start_api "$n"; done
    start_lb
    echo
    echo "waiting for the cluster..."
    for _ in $(seq 1 60); do
      if curl -fs "http://localhost:$(api_port 1)/api/health" >/dev/null 2>&1; then break; fi
      sleep 1
    done
    echo "ready.  Status:  curl -s http://localhost:$LB_PORT/api/cluster/status | python3 -m json.tool"
    ;;
  stop)
    for n in $(seq 1 $API_COUNT); do stop_proc "api-$n"; done
    for id in "${STORAGE_IDS[@]}"; do stop_proc "$id"; done
    if [ -f "$RUN/nginx.pid" ]; then kill "$(cat "$RUN/nginx.pid")" 2>/dev/null || true; rm -f "$RUN/nginx.pid"; echo "stopped nginx"; fi
    ;;
  status)
    curl -s "http://localhost:$LB_PORT/api/cluster/status" | python3 -m json.tool
    ;;
  kill-storage)  [ -n "${2:-}" ] || { echo "usage: kill-storage <s1..s5>"; exit 1; }; stop_proc "$2" ;;
  start-storage) [ -n "${2:-}" ] || { echo "usage: start-storage <s1..s5>"; exit 1; }; start_storage "$2" ;;
  kill-api)      [ -n "${2:-}" ] || { echo "usage: kill-api <1..$API_COUNT>"; exit 1; }; stop_proc "api-$2" ;;
  start-api)     [ -n "${2:-}" ] || { echo "usage: start-api <1..$API_COUNT>"; exit 1; }; start_api "$2" ;;
  logs)          tail -n 50 -f "$RUN/logs/${2:?usage: logs <node-id e.g. api-1 or s1>}.log" ;;
  *)
    sed -n '2,15p' "${BASH_SOURCE[0]}"
    ;;
esac

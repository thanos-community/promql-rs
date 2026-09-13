#!/usr/bin/env bash
# End to end against a real Thanos: two Prometheus replicas scraping each
# other, a Thanos Sidecar in front of each, the Go `thanos query` as the
# reference and thanos-query-rs next to it, both deduplicating along the
# `replica` label. The same requests go to both queriers and the JSON is
# diffed after normalizing what legitimately differs (result order, float
# noise, Thanos's empty "analysis" object).
#
# Needs: prometheus and thanos (binaries on PATH, or PROMETHEUS_BIN /
# THANOS_BIN; set THANOS_SRC to a Thanos checkout to `go build` it
# instead, which is also the fallback when no thanos is on PATH), jq,
# curl, cargo. Listens on 19000-19001 and 19090-19096.
#
#   scripts/e2e-thanos.sh                        # ~4 minutes, mostly warm-up
#   THANOS_SRC=~/src/github.com/thanos-io/thanos scripts/e2e-thanos.sh
#   WARMUP=300 RANGE=180 scripts/e2e-thanos.sh   # longer range queries
#
# The warm-up must outlast RANGE plus the widest rate() window by a good
# margin: a counter born inside the window extrapolates differently in
# Prometheus 2.x (the engine inside older Thanos releases) and 3.x (what
# promql-engine ports), so young series would show up as diffs.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
WARMUP=${WARMUP:-180}
RANGE=${RANGE:-90}
PROM_A_PORT=19000
PROM_B_PORT=19001
SIDECAR_A_GRPC=19090
SIDECAR_A_HTTP=19091
GO_HTTP=19092
GO_GRPC=19093
RS_HTTP=19094
SIDECAR_B_GRPC=19095
SIDECAR_B_HTTP=19096

cleanup() {
  local status=$?
  # shellcheck disable=SC2046
  kill $(jobs -p) 2>/dev/null || true
  wait 2>/dev/null || true
  if [ "$status" -eq 0 ]; then
    rm -rf "$WORK"
  else
    echo "logs kept in $WORK" >&2
  fi
}
trap cleanup EXIT

PROMETHEUS_BIN=${PROMETHEUS_BIN:-$(command -v prometheus || echo "$HOME/src/github.com/prometheus/prometheus/prometheus")}
if [ -n "${THANOS_BIN:-}" ]; then
  :
elif [ -n "${THANOS_SRC:-}" ] || ! command -v thanos >/dev/null; then
  THANOS_SRC=${THANOS_SRC:-$HOME/src/github.com/thanos-io/thanos}
  echo "building thanos from $THANOS_SRC"
  (cd "$THANOS_SRC" && go build -o "$WORK/thanos" ./cmd/thanos)
  THANOS_BIN="$WORK/thanos"
else
  THANOS_BIN=$(command -v thanos)
fi
[ -x "$PROMETHEUS_BIN" ] || { echo "no prometheus binary at $PROMETHEUS_BIN" >&2; exit 2; }
echo "thanos: $("$THANOS_BIN" --version 2>&1 | head -1)"
echo "prometheus: $("$PROMETHEUS_BIN" --version 2>&1 | head -1)"

echo "building thanos-query-rs"
RS_BIN=$(cd "$ROOT" && cargo build -p thanos-query-rs --message-format=json 2>/dev/null \
  | jq -r 'select(.reason == "compiler-artifact" and .executable != null and (.target.name == "thanos-query-rs")) | .executable' | tail -1)
[ -x "$RS_BIN" ] || { echo "could not find the built thanos-query-rs binary" >&2; exit 2; }

# Two replicas of one Prometheus, told apart by the `replica` external
# label, both scraping both; a Sidecar in front of each.
replica() { # name prometheus-port sidecar-grpc-port sidecar-http-port
  local name=$1 port=$2 grpc=$3 http=$4
  cat >"$WORK/prometheus-$name.yml" <<EOF
global:
  scrape_interval: 5s
  external_labels:
    replica: $name
scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ['localhost:$PROM_A_PORT', 'localhost:$PROM_B_PORT']
EOF
  "$PROMETHEUS_BIN" --config.file="$WORK/prometheus-$name.yml" --storage.tsdb.path="$WORK/tsdb-$name" \
    --web.listen-address="0.0.0.0:$port" >"$WORK/prometheus-$name.log" 2>&1 &
  "$THANOS_BIN" sidecar --prometheus.url="http://localhost:$port" --tsdb.path="$WORK/tsdb-$name" \
    --grpc-address="0.0.0.0:$grpc" --http-address="0.0.0.0:$http" >"$WORK/sidecar-$name.log" 2>&1 &
}
replica a "$PROM_A_PORT" "$SIDECAR_A_GRPC" "$SIDECAR_A_HTTP"
replica b "$PROM_B_PORT" "$SIDECAR_B_GRPC" "$SIDECAR_B_HTTP"
"$THANOS_BIN" query --endpoint="localhost:$SIDECAR_A_GRPC" --endpoint="localhost:$SIDECAR_B_GRPC" \
  --query.replica-label=replica --http-address="0.0.0.0:$GO_HTTP" \
  --grpc-address="0.0.0.0:$GO_GRPC" >"$WORK/query.log" 2>&1 &

wait_for() {
  for _ in $(seq 1 90); do
    if curl -fsS "$1" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "timed out waiting for $2 at $1" >&2
  tail -20 "$WORK"/*.log >&2 || true
  exit 1
}
wait_for "http://localhost:$PROM_A_PORT/-/ready" "prometheus a"
wait_for "http://localhost:$PROM_B_PORT/-/ready" "prometheus b"
wait_for "http://localhost:$SIDECAR_A_HTTP/-/ready" "sidecar a"
wait_for "http://localhost:$SIDECAR_B_HTTP/-/ready" "sidecar b"
wait_for "http://localhost:$GO_HTTP/-/ready" "thanos query"

# Ours learns the sidecars' Info before serving, so it starts once they
# are up.
"$RS_BIN" --endpoint="localhost:$SIDECAR_A_GRPC" --endpoint="localhost:$SIDECAR_B_GRPC" \
  --query-replica-label=replica --http-address="0.0.0.0:$RS_HTTP" \
  --endpoint-info-interval=5s >"$WORK/query-rs.log" 2>&1 &
wait_for "http://localhost:$RS_HTTP/api/v1/status/buildinfo" thanos-query-rs

echo "warming up for ${WARMUP}s so rate() windows have samples"
sleep "$WARMUP"

# Values are rounded to 9 decimals and results sorted by metric; the Go
# querier adds an empty "analysis" object we do not.
normalize() {
  jq -S '
    def round9: if type == "string" then (try (tonumber | . * 1e9 | round / 1e9 | tostring) catch .) else . end;
    (if (.data | type) == "object" then del(.data.analysis) else . end)
    | if (.data | type) == "object" and (.data.result | type) == "array" then
        .data.result |= (map(
          (if .values? then .values |= map([.[0], (.[1] | round9)]) else . end)
          | (if .value? then .value |= [.[0], (.[1] | round9)] else . end)
        ) | sort_by(.metric | tostring))
      else . end'
}

FAIL=0
compare() { # path query-string
  local path=$1 qs=$2 go_code rs_code
  go_code=$(curl -sS -o "$WORK/go.raw" -w '%{http_code}' "http://localhost:$GO_HTTP$path?$qs")
  rs_code=$(curl -sS -o "$WORK/rs.raw" -w '%{http_code}' "http://localhost:$RS_HTTP$path?$qs")
  normalize <"$WORK/go.raw" >"$WORK/go.json" || cp "$WORK/go.raw" "$WORK/go.json"
  normalize <"$WORK/rs.raw" >"$WORK/rs.json" || cp "$WORK/rs.raw" "$WORK/rs.json"
  if [ "$go_code" = "$rs_code" ] && diff -u "$WORK/go.json" "$WORK/rs.json"; then
    echo "ok   $go_code $path?$qs"
  else
    echo "DIFF go=$go_code rs=$rs_code $path?$qs"
    FAIL=1
  fi
}
enc() { jq -rn --arg q "$1" '$q | @uri'; }

END=$(( $(date +%s) - 15 ))
START=$(( END - RANGE ))

for q in \
  'up' \
  'sum by (job) (up)' \
  'count by (replica) (up)' \
  'rate(prometheus_http_requests_total[1m])' \
  'sum(rate(prometheus_http_requests_total[1m]))' \
  'count_over_time(up[10m])' \
  'max_over_time(process_resident_memory_bytes[5m])'; do
  compare /api/v1/query_range "query=$(enc "$q")&start=$START&end=$END&step=15"
done
for q in 'up' 'sum(up)' 'count(up)' 'rate(prometheus_http_requests_total[2m])'; do
  compare /api/v1/query "query=$(enc "$q")&time=$END"
done
# Deduplication switched off, or along another label, per request.
compare /api/v1/query_range "query=up&start=$START&end=$END&step=15&dedup=false"
compare /api/v1/query "query=$(enc 'count(up)')&time=$END&dedup=false"
compare /api/v1/query "query=$(enc 'count(up)')&time=$END&replicaLabels%5B%5D=instance"
compare /api/v1/labels "start=$START&end=$END"
compare /api/v1/labels "start=$START&end=$END&match%5B%5D=up"
compare /api/v1/label/job/values "start=$START&end=$END"
compare /api/v1/label/__name__/values "start=$START&end=$END&match%5B%5D=$(enc 'up')"

# Bad requests must fail the same way.
compare /api/v1/query_range "query=up&start=$START&end=$END&step=0"
compare /api/v1/query_range "query=up&start=$END&end=$START&step=15"
compare /api/v1/query "query=up&time=$END&engine=foo"
compare /api/v1/query "query=up&time=$END&dedup=maybe"

# Expected to diverge: the engine has no binary operators yet.
go_code=$(curl -sS -o /dev/null -w '%{http_code}' "http://localhost:$GO_HTTP/api/v1/query?query=$(enc 'up + 1')&time=$END")
rs_code=$(curl -sS -o /dev/null -w '%{http_code}' "http://localhost:$RS_HTTP/api/v1/query?query=$(enc 'up + 1')&time=$END")
echo "info go=$go_code rs=$rs_code /api/v1/query?query=up + 1 (expected to differ until binary operators land)"

if [ "$FAIL" -ne 0 ]; then
  echo "thanos-query-rs and thanos query disagree" >&2
  exit 1
fi
echo "thanos-query-rs matches thanos query on every compared request"

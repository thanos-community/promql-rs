#!/usr/bin/env bash
# Time the same requests on Go thanos query and thanos-query-rs, both
# fronting the same Thanos Store Gateway, so relative timings are
# comparable. Needs curl, jq, benchstat (go install
# golang.org/x/perf/cmd/benchstat@latest), and both queriers already
# running against that shared store.
#
#   scripts/time-thanos.sh
#   END=1789450000 scripts/time-thanos.sh 'sum(up)'
#   RUNS=3 RS=http://localhost:9902 scripts/time-thanos.sh
#
# Requests run strictly one at a time, never in parallel or backgrounded,
# and Go/Rust are interleaved within each iteration (Go then Rust, repeat)
# rather than run as two separate blocks: there is one shared Store
# Gateway behind one port-forward, so concurrent requests would time each
# other, and interleaving means both queriers see the same store/cache
# conditions across the run instead of one drifting relative to the
# other. Every sample is kept, not just a last "warm" one, because
# benchstat's median, confidence interval and p-value need the
# distribution to tell a real 15% gap from port-forward noise; a single
# sample can't do that. END defaults to six hours ago: the Store Gateway
# serves data that is hours old, and raw replica blocks only exist for
# about two days, so a fresh END or the replica-matcher query can
# silently return zero rows. Row counts are printed next to the timings
# because a faster answer with fewer rows is not actually faster.
set -uo pipefail

GO=${GO:-http://localhost:10912}
RS=${RS:-http://localhost:10902}
END=${END:-$(( $(date +%s) - 6*3600 ))}
RUNS=${RUNS:-10}
WORK=$(mktemp -d)

if ! command -v benchstat >/dev/null 2>&1
then
  echo "benchstat not found on PATH; install it with: go install golang.org/x/perf/cmd/benchstat@latest" >&2
  exit 1
fi

enc() {
  jq -rn --arg q "$1" '$q | @uri'
}

# Runs RUNS iterations, one curl at a time, Go then Rust each iteration,
# and keeps every sample as a benchstat-formatted line. Row counts come
# from the last iteration's response bodies.
row() { # label path qs name
  local label=$1 path=$2 qs=$3 name=$4 i t ns ng nr prefix
  for i in $(seq 1 "$RUNS")
  do
    t=$(curl -sS -m 300 -o "$WORK/go.json" -w '%{time_total}' "$GO$path?$qs")
    ns=$(awk -v t="$t" 'BEGIN{printf "%d\n", t*1e9}')
    printf 'Benchmark%s 1 %s ns/op\n' "$name" "$ns" >> "$WORK/go"

    t=$(curl -sS -m 300 -o "$WORK/rs.json" -w '%{time_total}' "$RS$path?$qs")
    ns=$(awk -v t="$t" 'BEGIN{printf "%d\n", t*1e9}')
    printf 'Benchmark%s 1 %s ns/op\n' "$name" "$ns" >> "$WORK/rs"
  done
  ng=$(jq -r '.data.result | length' "$WORK/go.json" 2>/dev/null)
  nr=$(jq -r '.data.result | length' "$WORK/rs.json" 2>/dev/null)
  prefix=""
  [ "${ng:-?}" = "${nr:-?}" ] || prefix="MISMATCH "
  printf '%srows %s go=%s rs=%s\n' "$prefix" "$label" "${ng:-?}" "${nr:-?}" >> "$WORK/rows.txt"
}

WRITERAW='sum(increase(grpc_server_handled_total{job="api",namespace="api",grpc_service="parca.profilestore.v1alpha1.ProfileStoreService",grpc_method="WriteRaw",grpc_code!~"Aborted|Unavailable|Internal|Unknown|Unimplemented|DataLoss|DeadlineExceeded"}[12h]))'
M='{prometheus_replica=~"prometheus-k8s-.+"}'

S6=$(( END - 6*3600 ))
S24=$(( END - 24*3600 ))
row 'WriteRaw increase[12h], instant' /api/v1/query "query=$(enc "$WRITERAW")&time=$END" 'WriteRawIncrease12h/instant'
row 'WriteRaw increase[12h], 6h @ 5m' /api/v1/query_range "query=$(enc "$WRITERAW")&start=$S6&end=$END&step=300" 'WriteRawIncrease12h/6h@5m'
row 'WriteRaw increase[12h], 24h @ 1m' /api/v1/query_range "query=$(enc "$WRITERAW")&start=$S24&end=$END&step=60" 'WriteRawIncrease12h/24h@1m'
row 'up, 6h @ 15s' /api/v1/query_range "query=up&start=$S6&end=$END&step=15" 'Up/6h@15s'
row 'count(up), 6h @ 15s' /api/v1/query_range "query=$(enc 'count(up)')&start=$S6&end=$END&step=15" 'CountUp/6h@15s'
row 'sum by (job) (rate(process_cpu_seconds_total[5m])), 6h @ 15s' /api/v1/query_range "query=$(enc 'sum by (job) (rate(process_cpu_seconds_total[5m]))')&start=$S6&end=$END&step=15" 'SumByJobRateProcessCPU5m/6h@15s'
row 'max_over_time(process_resident_memory_bytes[5m]), 6h @ 15s' /api/v1/query_range "query=$(enc 'max_over_time(process_resident_memory_bytes[5m])')&start=$S6&end=$END&step=15" 'MaxOverTimeResidentMemory5m/6h@15s'
row "rate(process_cpu_seconds_total$M[5m]), 6h @ 15s" /api/v1/query_range "query=$(enc "rate(process_cpu_seconds_total$M[5m])")&start=$S6&end=$END&step=15" 'RateProcessCPU5m/replicas/6h@15s'

# Extra queries the caller passes on the command line, each timed both
# ways since instant and range queries hit different code paths.
for q in "$@"
do
  clean=$(printf '%s' "$q" | tr -cd 'A-Za-z0-9_')
  row "$q, instant" /api/v1/query "query=$(enc "$q")&time=$END" "$clean/instant"
  row "$q, 6h @ 15s" /api/v1/query_range "query=$(enc "$q")&start=$S6&end=$END&step=15" "$clean/6h@15s"
done

cat "$WORK/rows.txt"
(cd "$WORK" && benchstat go rs)

rm -rf "$WORK"

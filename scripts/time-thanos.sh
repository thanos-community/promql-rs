#!/usr/bin/env bash
# Time the same requests on Go thanos query and thanos-query-rs, both
# fronting the same Thanos Store Gateway, so relative timings are
# comparable. Needs curl, jq, and both queriers already running against
# that shared store.
#
#   scripts/time-thanos.sh
#   END=1789450000 scripts/time-thanos.sh 'sum(up)'
#   RUNS=3 RS=http://localhost:9902 scripts/time-thanos.sh
#
# Each request runs RUNS times per querier and only the last is
# reported, so a cold-cache first hit does not skew the comparison.
# END defaults to six hours ago: the Store Gateway serves data that is
# hours old, and raw replica blocks only exist for the last ~2 days, so
# a fresh END or a replica-matcher query can silently return zero rows.
# Row counts are printed next to the timings because a fast answer with
# fewer rows is not actually faster. This is one warm run per cell, so
# treat differences under ~50ms as noise, and the port-forward to the
# Store Gateway is part of both timings, not isolated out.
set -uo pipefail

GO=${GO:-http://localhost:10912}
RS=${RS:-http://localhost:10902}
END=${END:-$(( $(date +%s) - 6*3600 ))}
RUNS=${RUNS:-2}
WORK=$(mktemp -d)

enc() {
  jq -rn --arg q "$1" '$q | @uri'
}

# Runs RUNS times, discarding all but the last, so store/page caches are
# warm for the timing that gets reported.
timeit() { # url out-file
  local i
  i=1
  while [ "$i" -lt "$RUNS" ]
  do
    curl -sS -m 300 -o /dev/null "$1"
    i=$((i+1))
  done
  curl -sS -m 300 -o "$2" -w '%{time_total}' "$1"
}

row() { # label path qs
  local label=$1 path=$2 qs=$3 tg tr ng nr
  tg=$(timeit "$GO$path?$qs" "$WORK/go.json")
  tr=$(timeit "$RS$path?$qs" "$WORK/rs.json")
  ng=$(jq -r '.data.result | length' "$WORK/go.json" 2>/dev/null)
  nr=$(jq -r '.data.result | length' "$WORK/rs.json" 2>/dev/null)
  printf '%-58s go %6.2fs  rs %6.2fs  rows go=%s rs=%s\n' "$label" "$tg" "$tr" "${ng:-?}" "${nr:-?}"
}

WRITERAW='sum(increase(grpc_server_handled_total{job="api",namespace="api",grpc_service="parca.profilestore.v1alpha1.ProfileStoreService",grpc_method="WriteRaw",grpc_code!~"Aborted|Unavailable|Internal|Unknown|Unimplemented|DataLoss|DeadlineExceeded"}[12h]))'
M='{prometheus_replica=~"prometheus-k8s-.+"}'

S6=$(( END - 6*3600 ))
S24=$(( END - 24*3600 ))
row 'WriteRaw increase[12h], instant' /api/v1/query "query=$(enc "$WRITERAW")&time=$END"
row 'WriteRaw increase[12h], 6h @ 5m' /api/v1/query_range "query=$(enc "$WRITERAW")&start=$S6&end=$END&step=300"
row 'WriteRaw increase[12h], 24h @ 1m' /api/v1/query_range "query=$(enc "$WRITERAW")&start=$S24&end=$END&step=60"
row 'up, 6h @ 15s' /api/v1/query_range "query=up&start=$S6&end=$END&step=15"
row 'count(up), 6h @ 15s' /api/v1/query_range "query=$(enc 'count(up)')&start=$S6&end=$END&step=15"
row 'sum by (job) (rate(process_cpu_seconds_total[5m])), 6h @ 15s' /api/v1/query_range "query=$(enc 'sum by (job) (rate(process_cpu_seconds_total[5m]))')&start=$S6&end=$END&step=15"
row 'max_over_time(process_resident_memory_bytes[5m]), 6h @ 15s' /api/v1/query_range "query=$(enc 'max_over_time(process_resident_memory_bytes[5m])')&start=$S6&end=$END&step=15"
row "rate(process_cpu_seconds_total$M[5m]), 6h @ 15s" /api/v1/query_range "query=$(enc "rate(process_cpu_seconds_total$M[5m])")&start=$S6&end=$END&step=15"

# Extra queries the caller passes on the command line, each timed both
# ways since instant and range queries hit different code paths.
for q in "$@"
do
  row "$q, instant" /api/v1/query "query=$(enc "$q")&time=$END"
  row "$q, 6h @ 15s" /api/v1/query_range "query=$(enc "$q")&start=$S6&end=$END&step=15"
done

rm -rf "$WORK"

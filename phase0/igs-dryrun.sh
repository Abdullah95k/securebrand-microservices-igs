#!/usr/bin/env bash
# Rehearses igs's Phase 0 end to end against a local fakeig: no proxies, no
# Instagram, under a minute. fakeig serves rotated doc_ids, so the run only
# passes if every step works: preflight, doc_id discovery from the page's
# bundle, capture and the parser check, smoke, the measurement run, the
# report (GO) and the bundle.
#
#   phase0/igs-dryrun.sh [extra igs-spike phase0 flags]
set -euo pipefail
cd "$(dirname "$0")/.."

GO=${GO:-go}
OUT=${OUT:-phase0/out-ig-phase0-dryrun}
ADDR=${ADDR:-127.0.0.1:8091}

"$GO" build -o bin/ ./cmd/fakeig ./cmd/igs-spike
rm -rf "$OUT"
mkdir -p "$OUT"
bin/fakeig --addr "$ADDR" --docids rotated --profiles 5 >"$OUT/fakeig.log" 2>&1 &
pid=$!
trap 'kill "$pid" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  if (exec 3<>"/dev/tcp/${ADDR%:*}/${ADDR#*:}") 2>/dev/null; then break; fi
  sleep 0.2
done

IGS_CONFIG="" bin/igs-spike phase0 --profiles phase0/igs-dryrun-profiles.txt --tiers direct --base "http://$ADDR" \
  --out-dir "$OUT" --rps 50 --parallel 2 --ip-echo-url "" "$@"

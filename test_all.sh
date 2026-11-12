#!/usr/bin/env bash
# Run every lesson end to end on this machine and print a PASS/FAIL table.
#   bash test_all.sh          all lessons
#   bash test_all.sh 1 3      only lessons 1 and 3
# Each lesson runs in a fresh temporary copy, so your own ./shopstream folders are untouched.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
LESSONS=("$@"); [ ${#LESSONS[@]} -eq 0 ] && LESSONS=(1 2 3 4 5 6 7)
WORK="$(mktemp -d)"; LOGS="$ROOT/test-logs"; mkdir -p "$LOGS"
echo "Python: $(python3 -V 2>&1) | Redis: $(command -v redis-server >/dev/null && redis-server --version | awk "{print \$3}" || echo missing) | $(uname -sr)"
printf '%-8s %-6s %-8s %s\n' lesson result seconds detail
fails=0
for n in "${LESSONS[@]}"; do
  d="$WORK/lesson-$n"; mkdir -p "$d"; cp "$ROOT/lessons/lesson-$n/"*.sh "$d/"
  t0=$(date +%s)
  ( cd "$d" && bash start.sh ) > "$LOGS/lesson-$n.log" 2>&1
  if out=$(cd "$d" && bash verify.sh 2>&1); then
    res=PASS; detail=$(echo "$out" | grep -o 'SHOP1-[A-Z0-9-]*' | tail -1)
  else
    res=FAIL; fails=$((fails+1)); detail=$(echo "$out" | tail -1)
  fi
  echo "$out" >> "$LOGS/lesson-$n.log"
  (cd "$d" && bash stop.sh >/dev/null 2>&1)
  printf '%-8s %-6s %-8s %s\n' "$n" "$res" "$(( $(date +%s) - t0 ))" "$detail"
done
rm -rf "$WORK"
echo; echo "Logs: $LOGS"
[ $fails -eq 0 ] && echo "All lessons passed." || echo "$fails lesson(s) failed — see the logs."
exit $fails

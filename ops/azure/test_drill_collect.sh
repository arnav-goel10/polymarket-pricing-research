#!/usr/bin/env bash
# Run the real deploy/azure/drill/drill-collect.sh against a fake node:
# a segment directory, an env file, and a fake curl for the identity token,
# the metrics and the blob uploads. Checks that a minute file the node
# renames from .prec.open to .prec in the middle of the collection is still
# uploaded once (the second rehearsal lost its collect on node-a to that),
# and that every failure ends with a `drill:` line vm_run.sh can report.
#
#   ops/azure/test_drill_collect.sh
#
# Linux only (the VM's bash, GNU stat). Run from test_dry_run.sh.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
collect="$here/../../deploy/azure/drill/drill-collect.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

fails=0
check() {
  if "$@"; then
    printf 'ok    %s\n' "$name"
  else
    printf 'FAIL  %s\n' "$name"
    fails=$((fails + 1))
  fi
}

mkdir -p "$tmp/bin"
# Uploads are recorded by blob name. FAKE_RENAME_ON names the upload during
# which the open file is closed, as the node does at a minute's end.
cat >"$tmp/bin/curl" <<'STUB'
#!/usr/bin/env bash
url=${*: -1}
case "$url" in
  *metadata/identity*) echo '{"access_token":"t0k3n"}' ;;
  */metrics) echo "poly_frames 7" ;;
  *)
    name=${url#*/archive/}
    if [ "${FAKE_FAIL_ON:-}" = "$name" ]; then
      echo "curl: (22) The requested URL returned error: 409" >&2
      exit 22
    fi
    echo "$name" >>"$FAKE_UPLOADS"
    if [ "${FAKE_RENAME_ON:-}" = "$name" ]; then
      mv "$FAKE_OPEN" "${FAKE_OPEN%.open}"
    fi
    ;;
esac
STUB
# install -o root needs root; the directories are all the test needs.
cat >"$tmp/bin/install" <<'STUB'
#!/usr/bin/env bash
dirs=()
while [ $# -gt 0 ]; do
  case "$1" in
    -o | -g | -m) shift 2 ;;
    -d) shift ;;
    *) dirs+=("$1"); shift ;;
  esac
done
mkdir -p "${dirs[@]}"
STUB
chmod +x "$tmp/bin/curl" "$tmp/bin/install"

setup() {
  rm -rf "$tmp/node" "$tmp/uploads"
  mkdir -p "$tmp/node/data/seg" "$tmp/node/state"
  printf 'POLY_NODE_ID=1\nPOLY_AZURE_CLIENT_ID=c\nPOLY_AZURE_STORE=https://example.invalid/\n' >"$tmp/node/node.env"
  # Slot 100000000 closed, 100000001 still open; 99999990 is outside.
  printf 'closed' >"$tmp/node/data/seg/100000000.aa.prec"
  printf 'writing' >"$tmp/node/data/seg/100000001.aa.prec.open"
  printf 'old' >"$tmp/node/data/seg/99999990.aa.prec"
  : >"$tmp/uploads"
}

run_collect() {
  set +e
  out=$(env PATH="$tmp/bin:$PATH" DRILL_ENV_FILE="$tmp/node/node.env" \
    DRILL_DATA="$tmp/node/data" DRILL_STATE_ROOT="$tmp/node/state" \
    FAKE_UPLOADS="$tmp/uploads" FAKE_OPEN="$tmp/node/data/seg/100000001.aa.prec.open" \
    "$@" bash "$collect" 1-1 6000000000000 6000000060000 2>&1)
  code=$?
  set -e
}

setup
run_collect
name="a plain collect uploads the window's segments and finishes"
check test "$code" = 0
check grep -qx "POLY_OK drill-collect" <<<"$out"
check grep -q "segments=2 " <<<"$out"
check grep -qx "drill/1-1/n1/seg/100000001.aa.prec.open.gz" "$tmp/uploads"
check test "$(grep -c '99999990' "$tmp/uploads")" = 0

setup
run_collect FAKE_RENAME_ON=drill/1-1/n1/seg/100000000.aa.prec.gz
name="a minute file closed during the collect is still uploaded, once, under its new name"
check test "$code" = 0
check grep -qx "POLY_OK drill-collect" <<<"$out"
check grep -q "segments=2 " <<<"$out"
check grep -qx "drill/1-1/n1/seg/100000001.aa.prec.gz" "$tmp/uploads"
check test "$(grep -c '100000001' "$tmp/uploads")" = 1

setup
run_collect FAKE_FAIL_ON=drill/1-1/n1/lease.jsonl
name="a failed upload ends with a drill line naming the blob and no finish marker"
check test "$code" != 0
check grep -q "^drill: upload of drill/1-1/n1/lease.jsonl failed" <<<"$out"
check test "$(grep -c POLY_OK <<<"$out")" = 0
name="vm_run reports that line, and no store address"
reason=$(bash "$here/vm_run.sh" node-a reason <<<"$out")
check grep -q "^drill: upload of drill/1-1/n1/lease.jsonl failed" <<<"$reason"
check test "$(grep -c example.invalid <<<"$reason")" = 0

setup
rm "$tmp/node/data/seg/100000001.aa.prec.open"
mkdir "$tmp/node/data/seg/100000001.aa.prec.open"
run_collect
name="a segment that cannot be read is named by its slot"
check test "$code" != 0
check grep -q "^drill: segment 100000001 could not be read" <<<"$out"

setup
printf '#!/usr/bin/env bash\nexit 3\n' >"$tmp/bin/mktemp"
chmod +x "$tmp/bin/mktemp"
run_collect
rm "$tmp/bin/mktemp"
name="any other failing command still says the stage it stopped at"
check test "$code" != 0
check grep -q "^drill: drill-collect.sh stopped at token (exit 3)" <<<"$out"

if [ "$fails" -gt 0 ]; then
  echo "$fails check(s) failed"
  exit 1
fi
echo "all drill-collect checks passed"

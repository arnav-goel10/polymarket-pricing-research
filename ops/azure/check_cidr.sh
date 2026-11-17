#!/usr/bin/env bash
# Accept an SSH source range only if it is one IPv4 range of /24 or
# narrower. Both ways of deploying the network call this: deploy.sh on the
# owner's machine and the ha-infra job of the workflow.
#
#   ops/azure/check_cidr.sh A.B.C.D/N
#
# Exits 0 when the range is fine. Otherwise it says why on stderr and exits
# 1. It never prints the range, because the workflow's log is public.
set -euo pipefail
cidr=${1:-}

no() {
  printf 'the SSH source range %s\n' "$*" >&2
  exit 1
}

[[ $cidr =~ ^([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})/([0-9]{1,2})$ ]] ||
  no "must be one IPv4 range written A.B.C.D/N"
for i in 1 2 3 4; do
  [ "$((10#${BASH_REMATCH[i]}))" -le 255 ] || no "has a part above 255"
done
bits=$((10#${BASH_REMATCH[5]}))
[ "$bits" -le 32 ] || no "has a prefix above /32"
[ "$bits" -ge 24 ] || no "must be /24 or narrower; a wider one opens SSH to addresses that are not yours"

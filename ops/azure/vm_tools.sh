#!/usr/bin/env bash
# Install the drill tools (deploy/azure/drill/) on a VM, or print their
# digest. The deploy workflow runs `install` on both nodes after the node
# itself, so every ha-deploy tag brings the VMs the drill scripts of its
# own commit; the drill job checks the digest before it opens anything.
#
#   ops/azure/vm_tools.sh digest          print the bundle's digest
#   ops/azure/vm_tools.sh install VM      install it on node-a or node-b
#
# The tools are not in cloud-init, because Azure refuses a change to a
# VM's custom data, which would break the next ha-infra tag. They go
# through the VM agent instead, as one script carrying the bundle. The
# digest is the sha256 of `sha256sum` over the files in name order, the
# same expression drill-lib.sh computes on the VM. `install` changes the VM
# and has no dry run; like the other scripts it refuses any subscription
# but the project's.
set -euo pipefail
# shellcheck source=ops/azure/lib.sh
. "$(dirname "$0")/lib.sh"
src="$REPO_ROOT/deploy/azure/drill"

digest() {
  (cd "$src" && export LC_ALL=C && sha256sum -- * | sha256sum | cut -c1-64)
}

cmd=${1:-}
case "$cmd" in
  digest)
    digest
    ;;
  install)
    vm=${2:?usage: vm_tools.sh install VM}
    case "$vm" in
      node-a | node-b) ;;
      *) echo "vm_tools: unknown VM $vm" >&2; exit 2 ;;
    esac
    require_subscription >&2
    want=$(digest)
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    bundle=$(cd "$src" && export LC_ALL=C && tar -czf - -- * | base64 | tr -d '\n')
    cat >"$tmp/install.sh" <<SCRIPT
# The VM agent runs this with sh, which on Ubuntu is dash and has no pipefail.
if [ -z "\${BASH_VERSION:-}" ]; then exec bash "\$0" "\$@"; fi
set -euo pipefail
d=/opt/poly/drill
new=\$(mktemp -d /opt/poly/drill.new.XXXXXX)
echo '$bundle' | base64 -d | tar -xzf - -C "\$new" --no-same-owner
got=\$(cd "\$new" && export LC_ALL=C && sha256sum -- * | sha256sum | cut -c1-64)
if [ "\$got" != "$want" ]; then
  rm -rf "\$new"
  echo "tools: digest mismatch"
  exit 1
fi
chown -R root:root "\$new"
chmod 0755 "\$new" "\$new"/*
rm -rf "\$d.old"
if [ -d "\$d" ]; then mv "\$d" "\$d.old"; fi
mv "\$new" "\$d"
rm -rf "\$d.old"
echo "tools \$got"
echo "POLY_OK tools"
SCRIPT
    out=$(az vm run-command invoke -g "$RG" -n "$vm" --command-id RunShellScript \
      --scripts "@$tmp/install.sh" --query "value[0].message" -o tsv)
    printf '%s\n' "$out" | grep -E '^(tools |POLY_OK )' || true
    grep -qx "POLY_OK tools" <<<"$out" || {
      echo "vm_tools: install on $vm did not finish" >&2
      exit 1
    }
    grep -qx "tools $want" <<<"$out" || {
      echo "vm_tools: $vm reports another digest" >&2
      exit 1
    }
    ;;
  *)
    echo "usage: vm_tools.sh digest | install VM" >&2
    exit 2
    ;;
esac

#!/bin/bash
# instrument.sh on|off -- (un)arm the cephadm agent-path instrumentation.
# Takes effect when the mgr next loads the module (seed.py fails it over).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
. "$here/env.sh"
mod=$MGR_PYTHON_PATH/cephadm
cp "$here/agentbench_instr.py" "$mod/agentbench_instr.py"
if ! grep -q agentbench_instr "$mod/module.py"; then
    printf '\nfrom . import agentbench_instr  # noqa: E402,F401\n' >> "$mod/module.py"
fi
mkdir -p "$AB/run"
case "${1:-on}" in
    on) touch "$AB/run/instrument" ;;
    off) rm -f "$AB/run/instrument" "$AB/run/instr.json" ;;
esac

#!/bin/bash
# ssh.sh N [command...]: root shell or command on nodeN (inside agentvm).
set -euo pipefail
. "$(dirname "$0")/env.sh"
i=$1
shift
vm_ssh "$i" "$@"

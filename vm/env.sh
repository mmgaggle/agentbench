# Shared settings for the agent test VMs (sourced inside the agentvm container).
VMS=/vms
BASE_URL=https://download.fedoraproject.org/pub/fedora/linux/releases/44/Cloud/x86_64/images
BASE_IMG=Fedora-Cloud-Base-Generic-44-1.7.x86_64.qcow2
BASE_SUM=Fedora-Cloud-44-1.7-x86_64-CHECKSUM
BRIDGE=br0
NET_PREFIX=10.77.0
GATEWAY=$NET_PREFIX.1
NODES=${NODES:-3}
VM_CPUS=${VM_CPUS:-2}
VM_MEM=${VM_MEM:-6144}
# bravo CCX1, away from the benchmark cluster on CCX0
VM_CPUSET=${VM_CPUSET:-8-15,24-31}
SSH_KEY=$VMS/id_ed25519

vm_ip() { echo "$NET_PREFIX.$((10 + $1))"; }
vm_mac() { printf '52:54:00:77:00:%02x' $((0x10 + $1)); }
vm_dir() { echo "$VMS/node$1"; }
vm_ssh() {
    local i=$1
    shift
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o LogLevel=ERROR -o ConnectTimeout=5 "root@$(vm_ip "$i")" "$@"
}

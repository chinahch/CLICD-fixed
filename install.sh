#!/bin/sh
set -eu

REPO="${CLICD_REPO:-chinahch/CLICD-fixed}"
CLICD_INSTALL_VERSION="${CLICD_VERSION:-latest}"
ASSET="clicd-linux-amd64.tar.gz"
ACTION="${1:-install}"

echo "====================================="
echo "  CLICD Installer"
echo "====================================="

log() {
    echo "[clicd] $*"
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

has_cmd() {
    command -v "$1" >/dev/null 2>&1
}

is_systemd() {
    has_cmd systemctl && [ -d /run/systemd/system ]
}

is_openrc() {
    has_cmd rc-service && has_cmd rc-update
}

if [ "$(id -u)" -ne 0 ]; then
    echo "Please run as root: sudo ./install.sh"
    echo "Or: curl -fsSL https://raw.githubusercontent.com/${REPO}/main/install.sh | sudo sh"
    echo "Uninstall: curl -fsSL https://raw.githubusercontent.com/${REPO}/main/install.sh | sudo sh -s -- uninstall"
    exit 1
fi

OS_ID="unknown"
OS_LIKE=""
if [ -r /etc/os-release ]; then
    . /etc/os-release
    OS_ID="${ID:-unknown}"
    OS_LIKE="${ID_LIKE:-}"
fi

usage() {
    cat << EOF
Usage:
  ./install.sh
  ./install.sh uninstall

Environment:
  CLICD_REPO=owner/repo
  CLICD_VERSION=latest|v1.0.0

Examples:
  curl -fsSL https://raw.githubusercontent.com/${REPO}/main/install.sh | sudo sh
  curl -fsSL https://raw.githubusercontent.com/${REPO}/main/install.sh | sudo sh -s -- uninstall
EOF
}

remove_path() {
    path="$1"
    if [ ! -e "$path" ] && [ ! -L "$path" ]; then
        return
    fi
    rm -rf "$path"
    log "Removed $path"
}

unmount_path_tree() {
    path="$1"
    if [ ! -e "$path" ]; then
        return
    fi

    if has_cmd findmnt; then
        findmnt -R -n -o TARGET "$path" 2>/dev/null | sort -r | while IFS= read -r mountpoint; do
            [ -n "$mountpoint" ] || continue
            umount -R -l "$mountpoint" >/dev/null 2>&1 || umount -l "$mountpoint" >/dev/null 2>&1 || true
        done
    fi

    umount -R -l "$path/rootfs" >/dev/null 2>&1 || umount -l "$path/rootfs" >/dev/null 2>&1 || true
    umount -R -l "$path" >/dev/null 2>&1 || umount -l "$path" >/dev/null 2>&1 || true
}

detach_container_loop_devices() {
    path="$1"
    if ! has_cmd losetup; then
        return
    fi

    for image in "$path"/rootfs.img "$path"/*.img; do
        [ -e "$image" ] || continue
        losetup -j "$image" 2>/dev/null | sed 's/:.*//' | while IFS= read -r loopdev; do
            [ -n "$loopdev" ] || continue
            losetup -d "$loopdev" >/dev/null 2>&1 || true
        done
    done
}

kill_path_users() {
    path="$1"
    if has_cmd fuser && [ -e "$path" ]; then
        fuser -km "$path" >/dev/null 2>&1 || true
    fi
}

remove_lxc_container_dir() {
    container_dir="$1"
    container_name="$(basename "$container_dir")"

    if has_cmd lxc-stop; then
        lxc-stop -n "$container_name" -k >/dev/null 2>&1 || true
    fi
    if has_cmd lxc-destroy; then
        lxc-destroy -n "$container_name" -f >/dev/null 2>&1 || true
    fi

    unmount_path_tree "$container_dir"
    detach_container_loop_devices "$container_dir"

    if rm -rf "$container_dir" >/dev/null 2>&1; then
        log "Removed $container_dir"
        return
    fi

    log "Retrying removal after terminating processes using $container_dir..."
    kill_path_users "$container_dir/rootfs"
    kill_path_users "$container_dir"
    unmount_path_tree "$container_dir"
    detach_container_loop_devices "$container_dir"
    rm -rf "$container_dir"
    log "Removed $container_dir"
}

remove_kvm_domain() {
    domain="$1"
    case "$domain" in
        vm-[0-9]*)
            ;;
        *)
            return
            ;;
    esac
    suffix="${domain#vm-}"
    case "$suffix" in
        ""|*[!0-9]*)
            return
            ;;
    esac
    if [ ! -d "/var/lib/clicd/kvm/instances/$domain" ] &&
        ! virsh dumpxml "$domain" 2>/dev/null | grep -q '/var/lib/clicd/kvm/'; then
        return
    fi

    log "Removing KVM domain $domain..."
    virsh destroy "$domain" >/dev/null 2>&1 || true
    virsh undefine "$domain" --remove-all-storage --nvram >/dev/null 2>&1 ||
        virsh undefine "$domain" --nvram >/dev/null 2>&1 ||
        virsh undefine "$domain" >/dev/null 2>&1 ||
        true
}

destroy_clicd_kvm_domains() {
    if ! has_cmd virsh; then
        return
    fi

    log "Destroying CLICD KVM domains..."
    virsh list --all --name 2>/dev/null | while IFS= read -r domain; do
        [ -n "$domain" ] || continue
        remove_kvm_domain "$domain"
    done
}

delete_iptables_lines() {
    table="$1"
    chain="$2"
    pattern="$3"
    if ! has_cmd iptables; then
        return
    fi

    while :; do
        line="$(iptables -t "$table" -L "$chain" -n --line-numbers 2>/dev/null | awk -v pat="$pattern" '$0 ~ pat {print $1; exit}')"
        [ -n "$line" ] || break
        iptables -t "$table" -D "$chain" "$line" >/dev/null 2>&1 || break
    done
}

delete_iptables_rule() {
    table="$1"
    shift
    if ! has_cmd iptables; then
        return
    fi

    while iptables -t "$table" -D "$@" >/dev/null 2>&1; do
        :
    done
}

delete_filter_rule() {
    if ! has_cmd iptables; then
        return
    fi

    while iptables -D "$@" >/dev/null 2>&1; do
        :
    done
}

delete_ip6tables_bridge_rules() {
    if ! has_cmd ip6tables; then
        return
    fi

    for bridge in lxcbr0 virbr0; do
        while :; do
            rule="$(ip6tables -S FORWARD 2>/dev/null | grep -- "$bridge" | sed 's/^-A /-D /' | head -n 1)"
            [ -n "$rule" ] || break
            # shellcheck disable=SC2086
            ip6tables $rule >/dev/null 2>&1 || break
        done
    done
}

cleanup_clicd_networking() {
    log "Cleaning CLICD firewall and bridge rules..."
    delete_iptables_lines nat PREROUTING 'clicd-'
    delete_iptables_rule nat POSTROUTING -s 10.0.3.0/24 -o eth+ -j MASQUERADE
    delete_iptables_rule nat POSTROUTING -s 192.168.122.0/24 -o eth+ -j MASQUERADE

    for bridge in lxcbr0 virbr0; do
        delete_filter_rule FORWARD -i "$bridge" -j ACCEPT
        delete_filter_rule FORWARD -o "$bridge" -j ACCEPT
        delete_filter_rule FORWARD -i "$bridge" -o "$bridge" -j ACCEPT
    done
    delete_ip6tables_bridge_rules
}

remove_clicd_host_hooks() {
    if has_cmd systemctl; then
        systemctl stop clicd-kvm-ipv6.service >/dev/null 2>&1 || true
        systemctl disable clicd-kvm-ipv6.service >/dev/null 2>&1 || true
    fi
    if has_cmd rc-service; then
        rc-service clicd-kvm-ipv6 stop >/dev/null 2>&1 || true
    fi
    if has_cmd rc-update; then
        rc-update del clicd-kvm-ipv6 default >/dev/null 2>&1 || true
    fi

    remove_path /usr/local/sbin/clicd-kvm-ipv6-init
    remove_path /etc/systemd/system/clicd-kvm-ipv6.service
    remove_path /etc/local.d/clicd-kvm-ipv6.start
    remove_path /etc/network/if-up.d/clicd-kvm-ipv6
}

remove_clicd_quota_records() {
    for file in /etc/projects /etc/projid; do
        [ -f "$file" ] || continue
        tmp="${file}.clicd-clean.$$"
        grep -v 'clicd-' "$file" > "$tmp" || true
        cat "$tmp" > "$file"
        rm -f "$tmp"
        log "Cleaned CLICD quota records from $file"
    done
}

remove_clicd_tmp_files() {
    for path in /tmp/clicd-* /tmp/clicd.*; do
        [ -e "$path" ] || [ -L "$path" ] || continue
        rm -rf "$path"
        log "Removed $path"
    done
}

remove_clicd_swapfile() {
    if [ ! -e /swapfile ]; then
        return
    fi
    swapoff /swapfile >/dev/null 2>&1 || true
    remove_path /swapfile
}

uninstall_clicd() {
    log "Uninstalling CLICD..."

    if has_cmd systemctl; then
        systemctl stop clicd >/dev/null 2>&1 || true
        systemctl disable clicd >/dev/null 2>&1 || true
    fi

    if has_cmd rc-service; then
        rc-service clicd stop >/dev/null 2>&1 || true
    fi
    if has_cmd rc-update; then
        rc-update del clicd default >/dev/null 2>&1 || true
    fi

    log "Destroying LXC containers under /var/lib/lxc..."
    for container_dir in /var/lib/lxc/*; do
        [ -d "$container_dir" ] || continue
        remove_lxc_container_dir "$container_dir"
    done
    destroy_clicd_kvm_domains
    cleanup_clicd_networking
    remove_clicd_host_hooks
    remove_clicd_quota_records

    remove_path /etc/systemd/system/clicd.service
    remove_path /etc/init.d/clicd
    remove_path /usr/local/bin/clicd
    remove_path /etc/sysctl.d/99-clicd.conf
    remove_path /var/log/clicd.log
    remove_path /var/log/clicd.err
    remove_path /root/.clicd
    unmount_path_tree /var/lib/lxc
    remove_path /var/lib/lxc
    unmount_path_tree /var/lib/clicd
    remove_path /var/lib/clicd
    remove_path /var/cache/lxc
    remove_path /var/cache/clicd
    remove_path /root/clicd-backups
    remove_clicd_tmp_files
    remove_clicd_swapfile

    if has_cmd systemctl; then
        systemctl daemon-reload >/dev/null 2>&1 || true
        systemctl reset-failed clicd >/dev/null 2>&1 || true
    fi
    if has_cmd sysctl; then
        sysctl --system >/dev/null 2>&1 || true
    fi

    echo ""
    echo "====================================="
    echo "  CLICD Uninstalled"
    echo "====================================="
    echo "  Removed service, binary, SQLite/config data, LXC containers,"
    echo "  CLICD KVM domains, VM images, image caches, firewall rules,"
    echo "  host hooks, quota records, temp files, backups, and swapfile."
    echo "====================================="
}

case "$ACTION" in
    install|"")
        ;;
    uninstall|remove)
        uninstall_clicd
        exit 0
        ;;
    -h|--help|help)
        usage
        exit 0
        ;;
    *)
        die "Unknown action: $ACTION"
        ;;
esac

install_apk() {
    log "Installing dependencies with apk..."
    apk update
    apk add --no-cache \
        ca-certificates \
        curl \
        wget \
        tar \
        gzip \
        xz \
        lxc \
        lxc-download \
        lxc-openrc \
        lxc-bridge \
        lxc-templates \
        bridge-utils \
        iproute2 \
        iptables \
        dnsmasq \
        dbus \
        qemu-system-x86_64 \
        qemu-img \
        libvirt \
        libvirt-daemon \
        libvirt-client \
        libvirt-qemu

    for pkg in lxcfs shadow conntrack-tools quota-tools e2fsprogs xfsprogs cloud-utils genisoimage xorriso; do
        apk add --no-cache "$pkg" >/dev/null 2>&1 || log "Optional package not installed: $pkg"
    done
}

install_apt() {
    log "Installing dependencies with apt..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y \
        ca-certificates \
        curl \
        wget \
        tar \
        gzip \
        xz-utils \
        lxc \
        lxc-templates \
        lxcfs \
        bridge-utils \
        uidmap \
        iproute2 \
        iptables \
        conntrack \
        quota \
        e2fsprogs \
        xfsprogs \
        dnsmasq-base \
        qemu-kvm \
        qemu-utils \
        libvirt-daemon-system \
        libvirt-clients \
        cloud-image-utils \
        genisoimage \
        xorriso \
        virtinst \
        ovmf
}

enable_el_repos() {
    if has_cmd dnf; then
        dnf install -y 'dnf-command(config-manager)' >/dev/null 2>&1 || true
        dnf install -y epel-release || true
        dnf config-manager --set-enabled crb >/dev/null 2>&1 || true
        dnf config-manager --set-enabled powertools >/dev/null 2>&1 || true
    elif has_cmd yum; then
        yum install -y yum-utils >/dev/null 2>&1 || true
        yum install -y epel-release || true
        yum-config-manager --enable powertools >/dev/null 2>&1 || true
    fi
}

install_dnf() {
    log "Installing dependencies with dnf..."
    enable_el_repos
    dnf install -y \
        ca-certificates \
        curl \
        wget \
        tar \
        gzip \
        xz \
        lxc \
        lxc-templates \
        bridge-utils \
        iproute \
        iptables \
        conntrack-tools \
        shadow-utils \
        quota \
        e2fsprogs \
        xfsprogs \
        dnsmasq \
        qemu-kvm \
        qemu-img \
        libvirt \
        libvirt-daemon-kvm \
        libvirt-client \
        virt-install \
        cloud-utils \
        genisoimage

    for pkg in lxcfs xorriso edk2-ovmf; do
        dnf install -y "$pkg" >/dev/null 2>&1 || log "Optional package not installed: $pkg"
    done
}

install_yum() {
    log "Installing dependencies with yum..."
    enable_el_repos
    yum install -y \
        ca-certificates \
        curl \
        wget \
        tar \
        gzip \
        xz \
        lxc \
        lxc-templates \
        bridge-utils \
        iproute \
        iptables \
        conntrack-tools \
        shadow-utils \
        quota \
        e2fsprogs \
        xfsprogs \
        dnsmasq \
        qemu-kvm \
        qemu-img \
        libvirt \
        libvirt-daemon-kvm \
        libvirt-client \
        virt-install \
        cloud-utils \
        genisoimage

    for pkg in lxcfs xorriso edk2-ovmf; do
        yum install -y "$pkg" >/dev/null 2>&1 || log "Optional package not installed: $pkg"
    done
}

install_dependencies() {
    case "$OS_ID" in
        ubuntu|debian)
            install_apt
            ;;
        alpine)
            install_apk
            ;;
        centos|rhel|rocky|almalinux|fedora)
            if has_cmd dnf; then
                install_dnf
            elif has_cmd yum; then
                install_yum
            else
                die "dnf/yum not found on $OS_ID"
            fi
            ;;
        *)
            if has_cmd apt-get; then
                install_apt
            elif has_cmd apk; then
                install_apk
            elif has_cmd dnf; then
                install_dnf
            elif has_cmd yum; then
                install_yum
            else
                die "Unsupported Linux distribution: ${OS_ID} ${OS_LIKE}"
            fi
            ;;
    esac

    has_cmd lxc-create || die "lxc-create is still missing after dependency installation."
    has_cmd iptables || die "iptables is still missing after dependency installation."
    has_cmd ip || die "iproute2/ip command is still missing after dependency installation."
    has_cmd virsh || die "virsh is still missing after dependency installation."
    has_cmd qemu-img || die "qemu-img is still missing after dependency installation."
    has_cmd cloud-localds || die "cloud-localds is still missing after dependency installation."
    if ! has_cmd genisoimage && ! has_cmd mkisofs && ! has_cmd xorriso; then
        die "one of genisoimage, mkisofs, or xorriso is required for Windows KVM setup."
    fi
    if [ ! -e /dev/kvm ]; then
        log "Warning: /dev/kvm was not found. KVM VMs require hardware virtualization or nested virtualization."
    fi
}

configure_kernel_networking() {
    log "Enabling kernel forwarding settings..."
    cat > /etc/sysctl.d/99-clicd.conf << 'EOF'
net.ipv4.ip_forward = 1
net.ipv6.conf.all.forwarding = 1
net.bridge.bridge-nf-call-iptables = 0
net.bridge.bridge-nf-call-ip6tables = 0
EOF

    modprobe br_netfilter >/dev/null 2>&1 || true
    sysctl --system >/dev/null 2>&1 || true
}

setup_runtime_services() {
    log "Configuring LXC and KVM services..."

    if is_systemd; then
        systemctl enable --now lxcfs >/dev/null 2>&1 || true
        systemctl enable --now lxc-net >/dev/null 2>&1 || true
        systemctl enable --now lxc >/dev/null 2>&1 || true
        systemctl enable --now libvirtd >/dev/null 2>&1 || true
        systemctl enable --now virtqemud >/dev/null 2>&1 || true
        systemctl enable --now virtqemud.socket >/dev/null 2>&1 || true
        systemctl enable --now virtlogd.socket >/dev/null 2>&1 || true
        return
    fi

    if is_openrc; then
        rc-update add cgroups default >/dev/null 2>&1 || true
        rc-service cgroups start >/dev/null 2>&1 || true
        rc-update add lxc default >/dev/null 2>&1 || true
        rc-service lxc start >/dev/null 2>&1 || true
        rc-update add lxcfs default >/dev/null 2>&1 || true
        rc-service lxcfs start >/dev/null 2>&1 || true
        rc-update add dbus default >/dev/null 2>&1 || true
        rc-service dbus start >/dev/null 2>&1 || true
        rc-update add libvirtd default >/dev/null 2>&1 || true
        rc-service libvirtd start >/dev/null 2>&1 || true
        rc-update add virtlogd default >/dev/null 2>&1 || true
        rc-service virtlogd start >/dev/null 2>&1 || true
        return
    fi

    die "No supported service manager found. CLICD supports systemd or OpenRC."
}

setup_subids() {
    log "Setting up subordinate UID/GID ranges..."
    touch /etc/subuid /etc/subgid
    grep -q '^root:' /etc/subuid 2>/dev/null || echo 'root:100000:65536' >> /etc/subuid
    grep -q '^root:' /etc/subgid 2>/dev/null || echo 'root:100000:65536' >> /etc/subgid
}

try_enable_project_quota() {
    root_src="$(findmnt -no SOURCE / 2>/dev/null || true)"
    root_fs="$(findmnt -no FSTYPE / 2>/dev/null || true)"

    if [ "$root_fs" != "ext4" ] || [ -z "$root_src" ] || [ ! -b "$root_src" ]; then
        log "Project quota auto-enable skipped for root filesystem: ${root_fs:-unknown}"
        return
    fi

    if ! has_cmd tune2fs; then
        log "Project quota auto-enable skipped because tune2fs is unavailable."
        return
    fi

    if tune2fs -l "$root_src" 2>/dev/null | grep -q 'project'; then
        log "Ext4 project quota support already appears to be enabled."
        return
    fi

    log "Ext4 project quota is not enabled. Disk limits will fall back to loopback images."
}

download_release_if_needed() {
    if [ -f "./clicd" ]; then
        return
    fi

    if [ "$CLICD_INSTALL_VERSION" = "latest" ]; then
        download_url="https://github.com/${REPO}/releases/latest/download/${ASSET}"
    else
        download_url="https://github.com/${REPO}/releases/download/${CLICD_INSTALL_VERSION}/${ASSET}"
    fi

    log "clicd binary not found in current directory."
    log "Downloading release package: ${download_url}"

    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' 0

    if has_cmd curl; then
        curl -fL "$download_url" -o "$tmp_dir/$ASSET"
    elif has_cmd wget; then
        wget -O "$tmp_dir/$ASSET" "$download_url"
    else
        die "curl or wget is required to download the release package."
    fi

    tar -xzf "$tmp_dir/$ASSET" -C "$tmp_dir"
    cd "$tmp_dir/clicd-linux-amd64"
    [ -f "./clicd" ] || die "Downloaded release package did not contain clicd."
}

install_binary() {
    if has_cmd systemctl; then
        systemctl stop clicd >/dev/null 2>&1 || true
    fi
    if has_cmd rc-service; then
        rc-service clicd stop >/dev/null 2>&1 || true
    fi

    tmp_bin="/usr/local/bin/clicd.new.$$"
    cp ./clicd "$tmp_bin"
    chmod +x "$tmp_bin"
    mv -f "$tmp_bin" /usr/local/bin/clicd
    chmod +x /usr/local/bin/clicd
    log "Installed binary: /usr/local/bin/clicd"
}

install_systemd_service() {
    cat > /etc/systemd/system/clicd.service << 'EOF'
[Unit]
Description=CLICD - LXC/KVM Container Manager
After=network.target lxc.service libvirtd.service virtqemud.service
Wants=libvirtd.service

[Service]
Type=simple
ExecStart=/usr/local/bin/clicd server
Restart=always
RestartSec=5
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable clicd
    systemctl restart clicd
}

install_openrc_service() {
    cat > /etc/init.d/clicd << 'EOF'
#!/sbin/openrc-run

name="CLICD"
description="CLICD - LXC/KVM Container Manager"
command="/usr/local/bin/clicd"
command_args="server"
command_background=true
pidfile="/run/clicd.pid"
output_log="/var/log/clicd.log"
error_log="/var/log/clicd.err"

depend() {
    need net
    after lxc libvirtd
}
EOF

    chmod +x /etc/init.d/clicd
    rc-update add clicd default
    rc-service clicd restart
}

install_service() {
    log "Installing CLICD service..."

    if is_systemd; then
        install_systemd_service
    elif is_openrc; then
        install_openrc_service
    else
        die "No supported service manager found. CLICD supports systemd or OpenRC."
    fi
}

print_summary() {
    echo ""
    echo "====================================="
    echo "  Installation Complete"
    echo "====================================="
    echo "  Web: http://YOUR_SERVER_IP:8999"
    echo "  Binary: /usr/local/bin/clicd"
    if is_systemd; then
        echo "  Service: systemctl {start|stop|restart|status} clicd"
        echo "  Logs: journalctl -u clicd -f"
    elif is_openrc; then
        echo "  Service: rc-service clicd {start|stop|restart|status}"
        echo "  Logs: tail -f /var/log/clicd.log /var/log/clicd.err"
    fi
    echo "====================================="
    echo ""
    echo "Initial credentials, if this was the first run:"
    if is_systemd; then
        journalctl -u clicd --no-pager -n 80 | grep -E "Username:|Password:" || true
    else
        grep -E "Username:|Password:" /var/log/clicd.log /var/log/clicd.err 2>/dev/null || true
    fi
    echo ""
    echo "If no password is shown, this server already had /root/.clicd/config.db."
    echo "The existing admin password cannot be recovered from the bcrypt hash."
}

install_dependencies
configure_kernel_networking
setup_runtime_services
setup_subids
try_enable_project_quota
download_release_if_needed
install_binary
install_service
sleep 2
print_summary

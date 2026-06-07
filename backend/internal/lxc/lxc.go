package lxc

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"clicd/internal/config"
)

// Manager handles LXC container operations
type Manager struct {
	LxcPath string
}

type containerUsageSample struct {
	CPUUsec    uint64
	RXBytes    uint64
	TXBytes    uint64
	ReadBytes  uint64
	WriteBytes uint64
	At         time.Time
}

// containerRateSnapshot stores computed rates, updated by a single background goroutine
type containerRateSnapshot struct {
	CPUPct    float64
	RXBps     float64
	TXBps     float64
	ReadBps   float64
	WriteBps  float64
	UpdatedAt time.Time
}

var (
	usageMu   sync.RWMutex
	lastUsage = map[string]containerUsageSample{}
	rateCache = map[string]containerRateSnapshot{}
)

var (
	sshEnsureLocks     sync.Map
	sshWarmupScheduled sync.Map
	sshWarmupSem       = make(chan struct{}, 4)
)

// StartUsageMonitor starts a background goroutine that computes rates every 5 seconds
func (m *Manager) StartUsageMonitor() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			m.updateAllRates()
		}
	}()
}

// WarmRunningContainersSSH prepares sshd for containers that were already running
// when clicd started, such as after host boot or service restart.
func (m *Manager) WarmRunningContainersSSH() {
	containers := append([]config.Container(nil), config.AppConfig.Containers...)
	for _, container := range containers {
		c := container
		if c.IsKVM() {
			continue
		}
		status, err := m.GetContainerStatus(c.LxcName())
		if err != nil || status != "running" {
			continue
		}
		config.UpdateContainerStatus(c.ID, "running")
		if c.IP != "" && m.containerPortListening(c.LxcName(), 22) {
			continue
		}
		m.WarmSSHAsync(c.ID, "running container scan")
	}
}

// StartSSHWarmupScanner repeatedly scans during service startup so containers
// that autostart slightly after clicd still get prepared before WebSSH opens.
func (m *Manager) StartSSHWarmupScanner() {
	go func() {
		for i := 0; i < 12; i++ {
			m.WarmRunningContainersSSH()
			time.Sleep(10 * time.Second)
		}
	}()
}

func (m *Manager) updateAllRates() {
	usageMu.Lock()
	defer usageMu.Unlock()

	for i := range config.AppConfig.Containers {
		c := &config.AppConfig.Containers[i]
		if c.IsKVM() {
			delete(lastUsage, c.VirshName())
			delete(rateCache, c.VirshName())
			continue
		}
		if c.Status != "running" {
			delete(lastUsage, c.LxcName())
			delete(rateCache, c.LxcName())
			continue
		}
		lxcName := c.LxcName()

		// Cache init PID once per scan so getContainerNetworkBytes / getContainerDiskIOBytes
		// don't each fork lxc-info separately.
		initPID := m.getContainerInitPID(lxcName)

		// Read memory from cgroup directly (no shell fork)
		memUsage := readCgroupFile(lxcName,
			"/sys/fs/cgroup/lxc/%s/memory.current",
			"/sys/fs/cgroup/lxc.payload.%s/memory.current",
			"/sys/fs/cgroup/memory/lxc/%s/memory.usage_in_bytes")

		// Read cpu usage from cgroup directly (no shell | awk fork)
		cpuUsec := readCgroupCPUUsec(lxcName,
			"/sys/fs/cgroup/lxc/%s/cpu.stat",
			"/sys/fs/cgroup/lxc.payload.%s/cpu.stat")

		rxBytes, txBytes := getNetworkBytesForPID(initPID)
		readBytes, writeBytes := getDiskIOBytesForPID(initPID)

		now := time.Now()
		sample := containerUsageSample{
			CPUUsec:    cpuUsec,
			RXBytes:    rxBytes,
			TXBytes:    txBytes,
			ReadBytes:  readBytes,
			WriteBytes: writeBytes,
			At:         now,
		}

		prev, exists := lastUsage[lxcName]
		lastUsage[lxcName] = sample

		rate := containerRateSnapshot{UpdatedAt: now}
		if exists {
			elapsed := sample.At.Sub(prev.At).Seconds()
			if elapsed > 0 && sample.CPUUsec >= prev.CPUUsec {
				rate.CPUPct = float64(sample.CPUUsec-prev.CPUUsec) / (elapsed * 1e6) * 100
			}
			if elapsed > 0 {
				if sample.RXBytes >= prev.RXBytes {
					rate.RXBps = float64(sample.RXBytes-prev.RXBytes) / elapsed
				}
				if sample.TXBytes >= prev.TXBytes {
					rate.TXBps = float64(sample.TXBytes-prev.TXBytes) / elapsed
				}
				if sample.ReadBytes >= prev.ReadBytes {
					rate.ReadBps = float64(sample.ReadBytes-prev.ReadBytes) / elapsed
				}
				if sample.WriteBytes >= prev.WriteBytes {
					rate.WriteBps = float64(sample.WriteBytes-prev.WriteBytes) / elapsed
				}
			}
		} else {
			// First sample: estimate from container uptime
			uptimeSec := m.getContainerUptimeSeconds(lxcName)
			if uptimeSec > 0 {
				rate.CPUPct = (float64(cpuUsec) / 1e6) / uptimeSec * 100
				rate.RXBps = float64(rxBytes) / uptimeSec
				rate.TXBps = float64(txBytes) / uptimeSec
				rate.ReadBps = float64(readBytes) / uptimeSec
				rate.WriteBps = float64(writeBytes) / uptimeSec
			}
		}

		// Fallback: if delta rate is 0 but cumulative has data, use cumulative average
		if rate.RXBps == 0 && rateCache[lxcName].RXBps > 0 {
			rate.RXBps = rateCache[lxcName].RXBps
		}
		if rate.TXBps == 0 && rateCache[lxcName].TXBps > 0 {
			rate.TXBps = rateCache[lxcName].TXBps
		}

		// Store memory as a rate field for convenience
		_ = memUsage
		rateCache[lxcName] = rate
	}

	// Clean up stale entries
	for name := range rateCache {
		found := false
		for i := range config.AppConfig.Containers {
			if config.AppConfig.Containers[i].LxcName() == name && config.AppConfig.Containers[i].Status == "running" {
				found = true
				break
			}
		}
		if !found {
			delete(rateCache, name)
			delete(lastUsage, name)
		}
	}
}

// NewManager creates a new LXC manager
func NewManager() *Manager {
	return &Manager{
		LxcPath: "/var/lib/lxc",
	}
}

// ContainerConfig defines container creation parameters
type ContainerConfig struct {
	Name             string  `json:"name"`
	Virtualization   string  `json:"virtualization,omitempty"`
	TemplateID       string  `json:"template_id"`
	VCPU             float64 `json:"vcpu"`
	CPUPercent       int     `json:"cpu_percent"`
	RAMMB            int     `json:"ram_mb"`
	DiskGB           int     `json:"disk_gb"`
	NetworkBWMbps    int     `json:"network_bw_mbps"`
	MonthlyTrafficGB int     `json:"monthly_traffic_gb"`
	TrafficMode      string  `json:"traffic_mode"`   // "total" or "in_out"
	TrafficInGB      int     `json:"traffic_in_gb"`  // 0=unlimited
	TrafficOutGB     int     `json:"traffic_out_gb"` // 0=unlimited
	IOSpeedMBps      int     `json:"io_speed_mbps"`
	ExtraPorts       []int   `json:"extra_ports"`
	PortMappingCount int     `json:"port_mapping_count"`
	SnapshotLimit    int     `json:"snapshot_limit"`
	AssignIPv6       bool    `json:"assign_ipv6"`
	ExpiresAt        string  `json:"expires_at"`
}

// CreateContainer creates a new LXC container. Uses ct-{id} as LXC name internally.
func (m *Manager) CreateContainer(cfg ContainerConfig) error {
	tmpl := FindTemplate(cfg.TemplateID)
	if tmpl == nil {
		return fmt.Errorf("template not found: %s", cfg.TemplateID)
	}
	if cfg.PortMappingCount < 2 {
		cfg.PortMappingCount = 2
	}
	if cfg.SnapshotLimit <= 0 {
		cfg.SnapshotLimit = config.DefaultSnapshotLimit
	}

	if !config.IsValidContainerName(cfg.Name) {
		return fmt.Errorf("invalid container name: %s", cfg.Name)
	}
	if config.FindContainerByName(cfg.Name) != nil {
		return fmt.Errorf("container name already exists: %s", cfg.Name)
	}

	// Allocate ID and build LXC name
	id := config.AllocateContainerID()
	lxcName := fmt.Sprintf("ct-%d", id)

	containerDir := filepath.Join(m.LxcPath, lxcName)
	if _, err := os.Stat(containerDir); err == nil {
		if err := m.cleanupContainerStorage(lxcName); err != nil {
			return fmt.Errorf("failed to clean stale container directory %s: %v", lxcName, err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to check container directory %s: %v", lxcName, err)
	}

	fmt.Printf("Creating LXC container: %s (ID=%d, template: %s/%s/%s)\n",
		lxcName, id, tmpl.Distro, tmpl.Release, tmpl.Arch)

	args := []string{"-n", lxcName, "-t", "download", "--",
		"-d", tmpl.Distro, "-r", tmpl.Release, "-a", tmpl.Arch}
	if tmpl.Variant != "" {
		args = append(args, "--variant", tmpl.Variant)
	}
	cmd := exec.Command("lxc-create", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("lxc-create failed: %v, output: %s", err, string(output))
	}

	if err := m.applyDiskLimit(lxcName, cfg.DiskGB); err != nil {
		_ = m.cleanupContainerStorage(lxcName)
		return err
	}

	// Apply resource limits and mandatory security hardening.
	if err := m.applyResourceLimits(lxcName, cfg); err != nil {
		_ = m.cleanupContainerStorage(lxcName)
		return err
	}

	ipv6 := ""
	ipv6PrefixLen := 0
	ipv6Interface := ""
	if cfg.AssignIPv6 {
		assigned, prefixLen, iface, err := m.allocateIPv6ForContainer(id)
		if err != nil {
			_ = m.cleanupContainerStorage(lxcName)
			return err
		}
		ipv6 = assigned
		ipv6PrefixLen = prefixLen
		ipv6Interface = iface
		if err := m.applyIPv6Config(lxcName, ipv6); err != nil {
			_ = m.cleanupContainerStorage(lxcName)
			return err
		}
	}

	sshPort := config.AllocateSSHPort()
	sshPassword := generateRandomString(16)

	// Setup default port mappings (SSH only)
	portMappings := SetupDefaultPortMappings(sshPort)
	tempC := &config.Container{PortMappings: portMappings}

	extraPorts := cfg.ExtraPorts
	if len(extraPorts) == 0 && cfg.PortMappingCount > 1 {
		extraPorts = allocateDefaultEqualPorts(tempC, cfg.PortMappingCount-1)
	}
	for _, containerPort := range extraPorts {
		if containerPort <= 0 {
			continue
		}
		pm, err := normalizePortMapping(tempC, -1, config.PortMapping{
			ContainerPort: containerPort,
			HostPort:      containerPort,
			Protocol:      "tcp",
			Description:   fmt.Sprintf("Port-%d", containerPort),
		})
		if err != nil {
			continue
		}
		tempC.PortMappings = append(tempC.PortMappings, pm)
		portMappings = tempC.PortMappings
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	// Determine traffic mode
	trafficMode := cfg.TrafficMode
	if trafficMode == "" {
		trafficMode = "total"
	}
	trafficResetDate := now[:7] // YYYY-MM for monthly tracking

	container := config.Container{
		ID:               id,
		UUID:             config.NewContainerUUID(),
		Name:             cfg.Name,
		Virtualization:   config.VirtualizationLXC,
		Template:         cfg.TemplateID,
		VCPU:             cfg.VCPU,
		RAMMB:            cfg.RAMMB,
		DiskGB:           cfg.DiskGB,
		NetworkBWMbps:    cfg.NetworkBWMbps,
		MonthlyTrafficGB: cfg.MonthlyTrafficGB,
		TrafficMode:      trafficMode,
		TrafficInGB:      cfg.TrafficInGB,
		TrafficOutGB:     cfg.TrafficOutGB,
		TrafficResetDate: trafficResetDate,
		IOSpeedMBps:      cfg.IOSpeedMBps,
		Status:           "stopped",
		IP:               "",
		IPv6:             ipv6,
		IPv6PrefixLen:    ipv6PrefixLen,
		IPv6Interface:    ipv6Interface,
		VNCPort:          0,
		SSHPort:          sshPort,
		SSHPassword:      sshPassword,
		PortMappings:     portMappings,
		PortMappingLimit: cfg.PortMappingCount,
		SnapshotLimit:    config.NormalizeSnapshotLimit(cfg.SnapshotLimit),
		CreatedAt:        now,
		ExpiresAt:        cfg.ExpiresAt,
	}
	config.AddContainer(container)

	// Pre-configure network and SSH in the rootfs before first boot.
	rootfsPath := filepath.Join(m.LxcPath, lxcName, "rootfs")
	m.preconfigureNetwork(rootfsPath, cfg.TemplateID)
	if ipv6 != "" {
		if err := installContainerIPv6Init(rootfsPath, ipv6); err != nil {
			fmt.Printf("Warning: failed to install IPv6 init in %s: %v\n", lxcName, err)
		}
	}
	if err := m.preconfigureSSH(rootfsPath, sshPassword, cfg.TemplateID); err != nil {
		fmt.Printf("Warning: failed to pre-configure SSH in %s: %v\n", lxcName, err)
	}

	if err := m.shiftRootfsForUnprivileged(lxcName); err != nil {
		_ = m.cleanupContainerStorage(lxcName)
		config.RemoveContainer(id)
		return err
	}

	// Set root password AFTER shiftRootfsForUnprivileged,
	// otherwise /etc/shadow ownership breaks and SSHD cannot authenticate.
	setCmd := m.rootfsCommand(rootfsPath,
		"sh", "-c", fmt.Sprintf("printf '%%s:%%s\\n' root %s | chpasswd", shellQuote(sshPassword)))
	setCmd.Run()

	fmt.Printf("Container %d (%s) created successfully\n", id, cfg.Name)
	return nil
}

func (m *Manager) preconfigureNetwork(rootfsPath, templateID string) {
	osRelease := ""
	if data, err := os.ReadFile(filepath.Join(rootfsPath, "etc", "os-release")); err == nil {
		osRelease = strings.ToLower(string(data))
	}
	isAlpine := strings.Contains(osRelease, "alpine")
	isRHELFamily := strings.Contains(osRelease, "centos") ||
		strings.Contains(osRelease, "rhel") ||
		strings.Contains(osRelease, "rocky") ||
		strings.Contains(osRelease, "alma") ||
		strings.Contains(osRelease, "fedora") ||
		strings.Contains(templateID, "fedora") ||
		strings.Contains(templateID, "rockylinux")

	if isAlpine {
		interfaces := filepath.Join(rootfsPath, "etc", "network", "interfaces")
		content := "auto lo\niface lo inet loopback\n\nauto eth0\niface eth0 inet dhcp\n"
		_ = os.MkdirAll(filepath.Dir(interfaces), 0755)
		_ = os.WriteFile(interfaces, []byte(content), 0644)
		_ = exec.Command("chroot", rootfsPath, "rc-update", "add", "networking", "boot").Run()
		return
	}

	if isRHELFamily || strings.Contains(templateID, "centos") {
		nmDir := filepath.Join(rootfsPath, "etc", "NetworkManager", "system-connections")
		if err := os.MkdirAll(nmDir, 0700); err == nil {
			keyfile := `[connection]
id=eth0
type=ethernet
interface-name=eth0
autoconnect=true

[ipv4]
method=auto

[ipv6]
method=ignore
`
			path := filepath.Join(nmDir, "eth0.nmconnection")
			_ = os.WriteFile(path, []byte(keyfile), 0600)
		}
		_ = exec.Command("chroot", rootfsPath, "systemctl", "enable", "NetworkManager").Run()
	}

	networkdDir := filepath.Join(rootfsPath, "etc", "systemd", "network")
	if err := os.MkdirAll(networkdDir, 0755); err == nil {
		network := `[Match]
Name=eth0

[Network]
DHCP=ipv4
IPv6AcceptRA=no
`
		_ = os.WriteFile(filepath.Join(networkdDir, "10-eth0.network"), []byte(network), 0644)
	}
	if !isRHELFamily {
		_ = exec.Command("chroot", rootfsPath, "systemctl", "enable", "systemd-networkd").Run()
	}
}

// preconfigureSSH installs and configures SSH directly in the rootfs before first boot.
func (m *Manager) preconfigureSSH(rootfsPath, password, templateID string) error {
	_ = templateID
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cmd := m.rootfsCommand(rootfsPath, "sh", "-c", sshSetupScript(password, false))
	cmd = exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	output, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("timed out after 180s, output: %s", string(output))
	}
	if err != nil {
		return fmt.Errorf("%v, output: %s", err, string(output))
	}
	fmt.Printf("SSH pre-configured in rootfs\n")
	return nil
}

// applyResourceLimits applies cgroup v2 limits and mandatory security hardening to container config.
func (m *Manager) applyResourceLimits(lxcName string, cfg ContainerConfig) error {
	configFile := filepath.Join(m.LxcPath, lxcName, "config")

	data, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("failed to read container config: %v", err)
	}
	content := string(data)

	lines := strings.Split(content, "\n")
	var newLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.Contains(trimmed, "# clicd managed") &&
			!strings.HasPrefix(trimmed, "lxc.cgroup2.memory.max") &&
			!strings.HasPrefix(trimmed, "lxc.cgroup2.cpuset.cpus") &&
			!strings.HasPrefix(trimmed, "lxc.cgroup2.cpu.max") &&
			!strings.HasPrefix(trimmed, "lxc.cgroup2.io.max") &&
			!strings.HasPrefix(trimmed, "lxc.mount.auto") &&
			!strings.HasPrefix(trimmed, "lxc.prlimit") &&
			!strings.HasPrefix(trimmed, "lxc.idmap") &&
			!strings.HasPrefix(trimmed, "lxc.apparmor.profile") &&
			!strings.HasPrefix(trimmed, "lxc.seccomp.profile") &&
			!strings.HasPrefix(trimmed, "lxc.no_new_privs") &&
			!strings.HasPrefix(trimmed, "lxc.cap.drop") {
			newLines = append(newLines, line)
		}
	}

	seccompProfile, err := findSeccompProfile()
	if err != nil {
		return err
	}
	apparmorProfile, err := findAppArmorProfile()
	if err != nil {
		return err
	}
	uidBase, gidBase, err := unprivilegedIDMap()
	if err != nil {
		return err
	}

	newLines = append(newLines, "", "# clicd managed: lxcfs virtualized /proc")
	newLines = append(newLines, "lxc.mount.auto = proc:mixed sys:mixed cgroup:mixed")
	newLines = append(newLines, "", "# clicd managed: mandatory unprivileged container hardening")
	newLines = append(newLines, fmt.Sprintf("lxc.idmap = u 0 %d 65536", uidBase))
	newLines = append(newLines, fmt.Sprintf("lxc.idmap = g 0 %d 65536", gidBase))
	newLines = append(newLines, fmt.Sprintf("lxc.apparmor.profile = %s", apparmorProfile))
	newLines = append(newLines, fmt.Sprintf("lxc.seccomp.profile = %s", seccompProfile))
	newLines = append(newLines, "lxc.no_new_privs = 1")
	// Keep sys_admin: unprivileged containers need it to mount tmpfs (/dev/shm, /run, etc.)
	// All capabilities are already confined to the container's user namespace.
	newLines = append(newLines, "lxc.cap.drop = mac_admin mac_override sys_module sys_rawio sys_time sys_boot sys_nice sys_resource sys_ptrace sys_pacct mknod audit_control audit_read")
	newLines = append(newLines, "lxc.prlimit.nofile = 1024:4096")
	newLines = append(newLines, "lxc.prlimit.nproc = 128:256")
	newLines = append(newLines, "", "# clicd managed resource limits (cgroup v2)")

	if cfg.VCPU > 0 {
		cpuPct := cfg.CPUPercent
		if cpuPct <= 0 || cpuPct > 100 {
			cpuPct = 100
		}
		cpuQuota := int(cfg.VCPU * float64(cpuPct) / 100.0 * 100000)
		newLines = append(newLines, fmt.Sprintf("lxc.cgroup2.cpu.max = %d 100000", cpuQuota))
	}
	if cfg.RAMMB > 0 {
		ramBytes := int64(cfg.RAMMB) * 1024 * 1024
		newLines = append(newLines, fmt.Sprintf("lxc.cgroup2.memory.max = %d", ramBytes))
	}
	if cfg.IOSpeedMBps > 0 {
		// Note: lxc.cgroup2.io.max is skipped for unprivileged containers because
		// LXC's cgfsng_setup_limits cannot resolve host device numbers (e.g. 8:1)
		// in the unprivileged namespace context.
		// IO limits are instead applied post-start via direct cgroup2 writes.
		fmt.Printf("Info: IO limit (%d MB/s) for %s will be applied post-start via cgroup2\n", cfg.IOSpeedMBps, lxcName)
	}

	newContent := strings.Join(newLines, "\n")
	if err := os.WriteFile(configFile, []byte(newContent), 0644); err != nil {
		return fmt.Errorf("failed to write container config: %v", err)
	}
	return nil
}

func (m *Manager) ioLimitLines(lxcName string, mbps int) ([]string, error) {
	if mbps <= 0 {
		return nil, nil
	}
	devices, err := m.rootfsBlockDevices(lxcName)
	if err != nil {
		return nil, err
	}
	ioBytes := mbps * 1024 * 1024
	lines := make([]string, 0, len(devices))
	for _, device := range devices {
		lines = append(lines, fmt.Sprintf("%s rbps=%d wbps=%d", device, ioBytes, ioBytes))
	}
	return lines, nil
}

func (m *Manager) rootfsBlockDevices(lxcName string) ([]string, error) {
	rootfsPath := filepath.Join(m.LxcPath, lxcName, "rootfs")
	out, err := exec.Command("findmnt", "-T", rootfsPath, "-no", "MAJ:MIN").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to detect rootfs block device for IO limit: %v", err)
	}
	re := regexp.MustCompile(`\b\d+:\d+\b`)
	matches := re.FindAllString(string(out), -1)
	seen := map[string]bool{}
	devices := make([]string, 0, len(matches))
	for _, match := range matches {
		if match == "0:0" || seen[match] {
			continue
		}
		seen[match] = true
		devices = append(devices, match)
	}
	if len(devices) == 0 {
		return nil, fmt.Errorf("failed to detect rootfs block device for IO limit; refusing to use hardcoded 8:0")
	}
	return devices, nil
}

func (m *Manager) applyDiskLimit(lxcName string, diskGB int) error {
	if diskGB <= 0 {
		return nil
	}
	return m.applyLoopbackDiskLimit(lxcName, diskGB)
}

func (m *Manager) applyProjectDiskLimit(lxcName string, diskGB int) error {
	if diskGB <= 0 {
		return nil
	}
	rootfsPath := filepath.Join(m.LxcPath, lxcName, "rootfs")
	if diskImageMounted(lxcName, rootfsPath) {
		return nil
	}
	fsType, err := findmntValue(rootfsPath, "FSTYPE")
	if err != nil {
		return fmt.Errorf("failed to detect rootfs filesystem for disk quota: %v", err)
	}
	fsType = strings.TrimSpace(fsType)

	switch fsType {
	case "btrfs":
		if err := exec.Command("btrfs", "quota", "enable", rootfsPath).Run(); err != nil {
			// btrfs returns an error when quota is already enabled on some versions.
			fmt.Printf("Warning: btrfs quota enable returned: %v\n", err)
		}
		output, err := exec.Command("btrfs", "qgroup", "limit", fmt.Sprintf("%dG", diskGB), rootfsPath).CombinedOutput()
		if err != nil {
			fmt.Printf("Warning: failed to apply btrfs disk quota, falling back to loopback rootfs: %v, output: %s\n", err, string(output))
			return m.applyLoopbackDiskLimit(lxcName, diskGB)
		}
		return nil
	case "xfs":
		if err := applyXFSProjectQuota(rootfsPath, lxcName, diskGB); err != nil {
			fmt.Printf("Warning: xfs project quota unavailable, falling back to loopback rootfs: %v\n", err)
			return m.applyLoopbackDiskLimit(lxcName, diskGB)
		}
		return nil
	case "ext4":
		if err := applyExt4ProjectQuota(rootfsPath, lxcName, diskGB); err != nil {
			fmt.Printf("Warning: ext4 project quota unavailable, falling back to loopback rootfs: %v\n", err)
			return m.applyLoopbackDiskLimit(lxcName, diskGB)
		}
		return nil
	default:
		fmt.Printf("Warning: unsupported rootfs filesystem %q for project quota, falling back to loopback rootfs\n", fsType)
		return m.applyLoopbackDiskLimit(lxcName, diskGB)
	}
}

func (m *Manager) applyLoopbackDiskLimit(lxcName string, diskGB int) error {
	containerDir := filepath.Join(m.LxcPath, lxcName)
	rootfsPath := filepath.Join(containerDir, "rootfs")
	imagePath := filepath.Join(containerDir, "rootfs.img")
	if diskImageMounted(lxcName, rootfsPath) {
		return nil
	}
	if _, err := os.Stat(imagePath); err == nil {
		return m.ensureDiskImageMounted(lxcName)
	}

	tmpMount := filepath.Join(containerDir, ".rootfs-image")
	backupRootfs := filepath.Join(containerDir, "rootfs.dir")
	if err := os.MkdirAll(tmpMount, 0755); err != nil {
		return err
	}
	defer os.RemoveAll(tmpMount)

	output, err := exec.Command("truncate", "-s", fmt.Sprintf("%dG", diskGB), imagePath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to create rootfs disk image: %v, output: %s", err, string(output))
	}
	output, err = exec.Command("mkfs.ext4", "-F", imagePath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to format rootfs disk image: %v, output: %s", err, string(output))
	}
	output, err = exec.Command("mount", "-o", "loop", imagePath, tmpMount).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to mount rootfs disk image: %v, output: %s", err, string(output))
	}
	mountedTmp := true
	defer func() {
		if mountedTmp {
			exec.Command("umount", "-l", tmpMount).Run()
		}
	}()

	output, err = exec.Command("cp", "-a", rootfsPath+string(os.PathSeparator)+".", tmpMount+string(os.PathSeparator)).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to copy rootfs into disk image: %v, output: %s", err, string(output))
	}
	if !rootfsHasInit(tmpMount) {
		return fmt.Errorf("failed to copy rootfs into disk image: init not found in prepared rootfs")
	}
	if output, err = exec.Command("umount", tmpMount).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to unmount prepared rootfs disk image: %v, output: %s", err, string(output))
	}
	mountedTmp = false

	if err := os.Rename(rootfsPath, backupRootfs); err != nil {
		return fmt.Errorf("failed to move original rootfs aside: %v", err)
	}
	if err := os.MkdirAll(rootfsPath, 0755); err != nil {
		os.Rename(backupRootfs, rootfsPath)
		return err
	}
	if err := m.ensureDiskImageMounted(lxcName); err != nil {
		os.RemoveAll(rootfsPath)
		os.Rename(backupRootfs, rootfsPath)
		return err
	}
	if err := os.RemoveAll(backupRootfs); err != nil {
		fmt.Printf("Warning: failed to remove old rootfs backup %s: %v\n", backupRootfs, err)
	}
	return nil
}

func (m *Manager) ensureDiskImageMounted(lxcName string) error {
	containerDir := filepath.Join(m.LxcPath, lxcName)
	rootfsPath := filepath.Join(containerDir, "rootfs")
	imagePath := filepath.Join(containerDir, "rootfs.img")
	if _, err := os.Stat(imagePath); os.IsNotExist(err) {
		return nil
	}
	if diskImageMounted(lxcName, rootfsPath) {
		return nil
	}
	if err := os.MkdirAll(rootfsPath, 0755); err != nil {
		return err
	}
	output, err := exec.Command("mount", "-o", "loop", imagePath, rootfsPath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to mount rootfs disk image: %v, output: %s", err, string(output))
	}
	return nil
}

func rootfsHasInit(rootfsPath string) bool {
	for _, rel := range []string{
		"sbin/init",
		"usr/lib/systemd/systemd",
		"lib/systemd/systemd",
		"bin/busybox",
		"bin/sh",
	} {
		if _, err := os.Stat(filepath.Join(rootfsPath, rel)); err == nil {
			return true
		}
	}
	// NixOS has init under a hash-named nix store path; check nix/store for any init
	nixStore := filepath.Join(rootfsPath, "nix", "store")
	if entries, err := os.ReadDir(nixStore); err == nil {
		for _, entry := range entries {
			if entry.IsDir() && strings.Contains(entry.Name(), "nixos-system-") {
				if _, err := os.Stat(filepath.Join(nixStore, entry.Name(), "init")); err == nil {
					return true
				}
				if _, err := os.Stat(filepath.Join(nixStore, entry.Name(), "systemd")); err == nil {
					return true
				}
			}
		}
	}
	return false
}

func diskImageMounted(lxcName, rootfsPath string) bool {
	_ = lxcName
	target, err := findmntValue(rootfsPath, "TARGET")
	if err != nil {
		return false
	}
	targetAbs, err := filepath.Abs(strings.TrimSpace(target))
	if err != nil {
		return false
	}
	rootfsAbs, err := filepath.Abs(rootfsPath)
	if err != nil {
		return false
	}
	return targetAbs == rootfsAbs
}

func applyXFSProjectQuota(rootfsPath, lxcName string, diskGB int) error {
	options, err := findmntValue(rootfsPath, "OPTIONS")
	if err != nil {
		return err
	}
	if !hasProjectQuotaOption(options) {
		return errors.New("xfs project quota is not enabled; remount with prjquota before creating containers")
	}
	mountPoint, err := findmntValue(rootfsPath, "TARGET")
	if err != nil {
		return err
	}
	projectID := projectQuotaID(lxcName)
	projectName := "clicd-" + lxcName
	if err := ensureProjectQuotaFiles(projectID, projectName, rootfsPath); err != nil {
		return err
	}
	output, err := exec.Command("xfs_quota", "-x",
		"-c", "project -s "+projectName,
		"-c", fmt.Sprintf("limit -p bhard=%dg %s", diskGB, projectName),
		mountPoint,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to apply xfs project quota: %v, output: %s", err, string(output))
	}
	return nil
}

func applyExt4ProjectQuota(rootfsPath, lxcName string, diskGB int) error {
	options, err := findmntValue(rootfsPath, "OPTIONS")
	if err != nil {
		return err
	}
	if !hasProjectQuotaOption(options) {
		return errors.New("ext4 project quota is not enabled; remount with prjquota before creating containers")
	}
	mountPoint, err := findmntValue(rootfsPath, "TARGET")
	if err != nil {
		return err
	}
	projectID := projectQuotaID(lxcName)
	output, err := exec.Command("chattr", "-p", strconv.Itoa(projectID), rootfsPath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to assign ext4 project id: %v, output: %s", err, string(output))
	}
	hardKB := diskGB * 1024 * 1024
	output, err = exec.Command("setquota", "-P", strconv.Itoa(projectID), "0", strconv.Itoa(hardKB), "0", "0", mountPoint).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to apply ext4 project quota: %v, output: %s", err, string(output))
	}
	return nil
}

func findmntValue(path, field string) (string, error) {
	out, err := exec.Command("findmnt", "-T", path, "-no", field).Output()
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(out))
	if value == "" {
		return "", fmt.Errorf("findmnt returned empty %s for %s", field, path)
	}
	return value, nil
}

func hasProjectQuotaOption(options string) bool {
	for _, option := range strings.Split(options, ",") {
		option = strings.TrimSpace(option)
		if option == "prjquota" || option == "pquota" || option == "project" {
			return true
		}
	}
	return false
}

func projectQuotaID(lxcName string) int {
	idPart := strings.TrimPrefix(lxcName, "ct-")
	id, err := strconv.Atoi(idPart)
	if err != nil {
		id = 1
	}
	return 200000 + id
}

func ensureProjectQuotaFiles(projectID int, projectName, rootfsPath string) error {
	projectsLine := fmt.Sprintf("%d:%s", projectID, rootfsPath)
	if err := appendUniqueLine("/etc/projects", projectsLine); err != nil {
		return err
	}
	projidLine := fmt.Sprintf("%s:%d", projectName, projectID)
	return appendUniqueLine("/etc/projid", projidLine)
}

func appendUniqueLine(path, line string) error {
	data, _ := os.ReadFile(path)
	for _, existing := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(existing) == line {
			return nil
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if len(data) > 0 && !strings.HasSuffix(string(data), "\n") {
		if _, err := f.WriteString("\n"); err != nil {
			return err
		}
	}
	_, err = f.WriteString(line + "\n")
	return err
}

func findSeccompProfile() (string, error) {
	for _, path := range []string{
		"/usr/share/lxc/config/common.seccomp",
		"/usr/share/lxc/config/common.seccomp.policy",
		"/etc/lxc/common.seccomp",
	} {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}
	return "", errors.New("required LXC seccomp profile not found")
}

func findAppArmorProfile() (string, error) {
	data, err := os.ReadFile("/sys/kernel/security/apparmor/profiles")
	if err != nil {
		return "", fmt.Errorf("apparmor is required but not available: %v", err)
	}
	profiles := string(data)
	for _, profile := range []string{"lxc-container-default-cgns", "lxc-container-default"} {
		if strings.Contains(profiles, profile+" ") || strings.Contains(profiles, profile+" (") {
			return profile, nil
		}
	}
	return "", errors.New("required LXC AppArmor profile not loaded")
}

func unprivilegedIDMap() (int, int, error) {
	if err := ensureSubIDRange("/etc/subuid", "root", 100000, 65536); err != nil {
		return 0, 0, err
	}
	if err := ensureSubIDRange("/etc/subgid", "root", 100000, 65536); err != nil {
		return 0, 0, err
	}
	uidBase, err := parseSubIDRange("/etc/subuid", "root")
	if err != nil {
		return 0, 0, err
	}
	gidBase, err := parseSubIDRange("/etc/subgid", "root")
	if err != nil {
		return 0, 0, err
	}
	return uidBase, gidBase, nil
}

func ensureSubIDRange(path, user string, start, count int) error {
	if _, err := parseSubIDRange(path, user); err == nil {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to update %s: %v", path, err)
	}
	defer f.Close()
	if _, err := f.WriteString(fmt.Sprintf("%s:%d:%d\n", user, start, count)); err != nil {
		return fmt.Errorf("failed to update %s: %v", path, err)
	}
	return nil
}

func parseSubIDRange(path, user string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("failed to read %s: %v", path, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Split(strings.TrimSpace(line), ":")
		if len(fields) != 3 || fields[0] != user {
			continue
		}
		start, startErr := strconv.Atoi(fields[1])
		count, countErr := strconv.Atoi(fields[2])
		if startErr == nil && countErr == nil && count >= 65536 {
			return start, nil
		}
	}
	return 0, fmt.Errorf("%s must contain a %s subordinate id range with at least 65536 ids", path, user)
}

func (m *Manager) shiftRootfsForUnprivileged(lxcName string) error {
	uidBase, gidBase, err := unprivilegedIDMap()
	if err != nil {
		return err
	}
	rootfsPath := filepath.Join(m.LxcPath, lxcName, "rootfs")
	marker := filepath.Join(rootfsPath, ".clicd-unprivileged-shifted")
	if _, err := os.Stat(marker); err == nil {
		return nil
	}
	m.unmountRootfsChildMounts(rootfsPath)
	var rootDev uint64
	rootInfo, err := os.Lstat(rootfsPath)
	if err != nil {
		fmt.Printf("Warning: failed to stat rootfs %s before ownership shift: %v; continuing without cross-device guard\n", rootfsPath, err)
	} else {
		switch rootStat := rootInfo.Sys().(type) {
		case *unix.Stat_t:
			rootDev = uint64(rootStat.Dev)
		case *syscall.Stat_t:
			rootDev = uint64(rootStat.Dev)
		default:
			fmt.Printf("Warning: failed to read rootfs device for %s; continuing without cross-device guard\n", rootfsPath)
		}
	}

	if err := filepath.WalkDir(rootfsPath, func(path string, _ os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		var pathDev uint64
		var uid int
		var gid int
		switch stat := info.Sys().(type) {
		case *unix.Stat_t:
			pathDev = uint64(stat.Dev)
			uid = int(stat.Uid)
			gid = int(stat.Gid)
		case *syscall.Stat_t:
			pathDev = uint64(stat.Dev)
			uid = int(stat.Uid)
			gid = int(stat.Gid)
		default:
			fmt.Printf("Warning: failed to read uid/gid for %s; skipping ownership shift for this path\n", path)
			if info.IsDir() && path != rootfsPath {
				return filepath.SkipDir
			}
			return nil
		}
		if rootDev != 0 && path != rootfsPath && pathDev != rootDev {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if uid >= uidBase && uid < uidBase+65536 && gid >= gidBase && gid < gidBase+65536 {
			return nil
		}
		if uid >= 0 && uid < 65536 {
			uid += uidBase
		}
		if gid >= 0 && gid < 65536 {
			gid += gidBase
		}
		return unix.Lchown(path, uid, gid)
	}); err != nil {
		return fmt.Errorf("failed to shift rootfs ownership for unprivileged LXC: %v", err)
	}

	if err := os.WriteFile(marker, []byte("1\n"), 0644); err != nil {
		return err
	}
	if err := unix.Lchown(marker, uidBase, gidBase); err != nil {
		return err
	}

	// Fix container directory permissions: unprivileged init runs as ns UID 0
	// (host UID 100000), which is "other" on the host. lxc-create sets the
	// container dir to 770, so we need o+x to let the container process
	// traverse into the directory and access rootfs.
	containerDir := filepath.Join(m.LxcPath, lxcName)
	if err := os.Chmod(containerDir, 0771); err != nil {
		return fmt.Errorf("failed to fix container directory permissions: %v", err)
	}
	return nil
}

func (m *Manager) unmountRootfsChildMounts(rootfsPath string) {
	rootAbs, err := filepath.Abs(rootfsPath)
	if err != nil {
		return
	}
	out, err := exec.Command("findmnt", "-R", "-n", "-o", "TARGET", rootfsPath).Output()
	if err != nil {
		return
	}
	targets := strings.Split(strings.TrimSpace(string(out)), "\n")
	for i, j := 0, len(targets)-1; i < j; i, j = i+1, j-1 {
		targets[i], targets[j] = targets[j], targets[i]
	}
	for _, target := range targets {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		targetAbs, err := filepath.Abs(target)
		if err != nil || targetAbs == rootAbs {
			continue
		}
		if strings.HasPrefix(targetAbs, rootAbs+string(os.PathSeparator)) {
			exec.Command("umount", "-R", "-l", targetAbs).Run()
		}
	}
}

func (m *Manager) rootfsShifted(lxcName string) bool {
	marker := filepath.Join(m.LxcPath, lxcName, "rootfs", ".clicd-unprivileged-shifted")
	_, err := os.Stat(marker)
	return err == nil
}

func (m *Manager) hasUnprivilegedIDMap(lxcName string) bool {
	data, err := os.ReadFile(filepath.Join(m.LxcPath, lxcName, "config"))
	if err != nil {
		return false
	}
	content := string(data)
	return strings.Contains(content, "lxc.idmap = u 0 ") && strings.Contains(content, "lxc.idmap = g 0 ")
}

// StartContainer starts an LXC container by its ID
func (m *Manager) StartContainer(id int) error {
	c := config.FindContainer(id)
	if c == nil {
		return fmt.Errorf("container not found: %d", id)
	}
	lxcName := c.LxcName()

	if err := m.ensureDiskImageMounted(lxcName); err != nil {
		return err
	}
	if m.hasUnprivilegedIDMap(lxcName) && !m.rootfsShifted(lxcName) {
		if err := m.shiftRootfsForUnprivileged(lxcName); err != nil {
			return err
		}
	}
	if m.rootfsShifted(lxcName) {
		if err := m.applyResourceLimits(lxcName, ContainerConfig{
			Name:             c.Name,
			TemplateID:       c.Template,
			VCPU:             c.VCPU,
			RAMMB:            c.RAMMB,
			DiskGB:           c.DiskGB,
			NetworkBWMbps:    c.NetworkBWMbps,
			MonthlyTrafficGB: c.MonthlyTrafficGB,
			IOSpeedMBps:      c.IOSpeedMBps,
			AssignIPv6:       c.IPv6 != "",
			ExpiresAt:        c.ExpiresAt,
		}); err != nil {
			return err
		}
	}
	if c.IPv6 != "" {
		if err := m.applyIPv6Config(lxcName, c.IPv6); err != nil {
			return err
		}
		if err := m.ApplyIPv6(id); err != nil {
			return err
		}
	}

	logFile := filepath.Join(os.TempDir(), "clicd-"+lxcName+"-start.log")
	os.Remove(logFile)
	cmd := exec.Command("lxc-start", "-n", lxcName, "-d", "--logfile", logFile, "--logpriority", "DEBUG")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to start container: %v, output: %s, lxc log: %s", err, string(output), tailFile(logFile, 80))
	}

	config.UpdateContainerStatus(id, "running")

	var ip string
	for retry := 0; retry < 10; retry++ {
		time.Sleep(2 * time.Second)
		ip, err = m.GetContainerIP(lxcName)
		if err == nil && ip != "" {
			break
		}
	}
	if ip != "" {
		c = config.FindContainer(id)
		if c != nil {
			c.IP = ip
			config.SaveConfig()
		}
	}
	if ip == "" {
		if repairedIP, repairErr := m.EnsureContainerIPv4(id); repairErr == nil && repairedIP != "" {
			ip = repairedIP
			c = config.FindContainer(id)
		} else if repairErr != nil {
			fmt.Printf("Warning: failed to prepare IPv4 for %s: %v\n", lxcName, repairErr)
		}
	}

	if ip != "" {
		if err := m.EnsureSSH(id); err != nil {
			return err
		}
	}

	if current := config.FindContainer(id); current != nil {
		c = current
	}
	if err := m.ApplyContainerLimits(c); err != nil {
		fmt.Printf("Warning: failed to apply runtime resource limits for %s: %v\n", lxcName, err)
	}

	if err := m.ApplyPortMappings(id); err != nil {
		fmt.Printf("Warning: failed to apply port mappings: %v\n", err)
	}
	if c.IPv6 != "" {
		if err := m.ApplyIPv6(id); err != nil {
			fmt.Printf("Warning: failed to apply IPv6 routing for %s: %v\n", lxcName, err)
		}
	}

	fmt.Printf("Container %d (%s) started, IP: %s\n", id, c.Name, ip)
	return nil
}

// applyBandwidthLimit applies tc-based bandwidth limit on container's veth interface
// ApplyContainerLimits re-applies resource limits (CPU, RAM, IO, BW) to a running container.
func (m *Manager) ApplyContainerLimits(c *config.Container) error {
	if c == nil || c.Status != "running" {
		return nil
	}
	lxcName := c.LxcName()

	// CPU: write cpu.max
	cpuQuota := int(c.VCPU * 100000)
	cpuLine := fmt.Sprintf("%d 100000", cpuQuota)
	for _, path := range []string{
		fmt.Sprintf("/sys/fs/cgroup/lxc/%s/cpu.max", lxcName),
		fmt.Sprintf("/sys/fs/cgroup/lxc.payload.%s/cpu.max", lxcName),
	} {
		os.WriteFile(path, []byte(cpuLine), 0644)
	}

	// Memory: write memory.max
	ramBytes := int64(c.RAMMB) * 1024 * 1024
	memLine := fmt.Sprintf("%d", ramBytes)
	for _, path := range []string{
		fmt.Sprintf("/sys/fs/cgroup/lxc/%s/memory.max", lxcName),
		fmt.Sprintf("/sys/fs/cgroup/lxc.payload.%s/memory.max", lxcName),
	} {
		os.WriteFile(path, []byte(memLine), 0644)
	}

	// IO speed: write io.max
	if c.IOSpeedMBps > 0 {
		ioLines, err := m.ioLimitLines(lxcName, c.IOSpeedMBps)
		if err != nil {
			return err
		}
		ioLine := strings.Join(ioLines, "\n")
		for _, path := range []string{
			fmt.Sprintf("/sys/fs/cgroup/lxc/%s/io.max", lxcName),
			fmt.Sprintf("/sys/fs/cgroup/lxc.payload.%s/io.max", lxcName),
		} {
			os.WriteFile(path, []byte(ioLine), 0644)
		}
	}

	// Network bandwidth
	if c.NetworkBWMbps > 0 {
		m.applyBandwidthLimit(lxcName, c.NetworkBWMbps)
	} else {
		m.cleanupBandwidthLimit(lxcName)
	}
	return nil
}

func (m *Manager) applyBandwidthLimit(lxcName string, mbps int) {
	veth := m.getContainerVethByNS(lxcName)
	if veth == "" {
		fmt.Printf("Warning: could not find veth for %s\n", lxcName)
		return
	}
	rate := fmt.Sprintf("%dmbit", mbps)
	burst := fmt.Sprintf("%dkbit", mbps*100)
	exec.Command("tc", "qdisc", "del", "dev", veth, "root").Run()
	exec.Command("tc", "qdisc", "add", "dev", veth, "root", "handle", "1:", "htb", "default", "10").Run()
	exec.Command("tc", "class", "add", "dev", veth, "parent", "1:", "classid", "1:10", "htb", "rate", rate, "burst", burst).Run()
	fmt.Printf("Bandwidth limit: %s = %d Mbps on %s\n", lxcName, mbps, veth)
}

func (m *Manager) cleanupBandwidthLimit(lxcName string) {
	veth := m.getContainerVethByNS(lxcName)
	if veth != "" {
		exec.Command("tc", "qdisc", "del", "dev", veth, "root").Run()
	}
}

func (m *Manager) getContainerVethByNS(lxcName string) string {
	pid := m.getContainerInitPID(lxcName)
	if pid == "" {
		return ""
	}
	cmd := exec.Command("sh", "-c",
		fmt.Sprintf("nsenter -t %s -n ip -o link show 2>/dev/null | grep -oP 'eth0@if\\K[0-9]+'", pid))
	out, _ := cmd.Output()
	ifIdx := strings.TrimSpace(string(out))
	if ifIdx == "" {
		return ""
	}
	cmd2 := exec.Command("sh", "-c",
		fmt.Sprintf("ip -o link show | grep '^%s:' | grep -oP 'veth[^:@]+'", ifIdx))
	out2, _ := cmd2.Output()
	return strings.TrimSpace(string(out2))
}

// StopContainer stops an LXC container by its ID
func (m *Manager) StopContainer(id int) error {
	c := config.FindContainer(id)
	if c == nil {
		return fmt.Errorf("container not found: %d", id)
	}
	lxcName := c.LxcName()

	status, _ := m.GetContainerStatus(lxcName)
	if status != "running" {
		config.UpdateContainerStatus(id, "stopped")
		m.CleanPortMappings(id)
		m.cleanupBandwidthLimit(lxcName)
		return nil
	}

	m.CleanPortMappings(id)
	m.cleanupBandwidthLimit(lxcName)

	cmd := exec.Command("lxc-stop", "-n", lxcName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "not running") {
			config.UpdateContainerStatus(id, "stopped")
			return nil
		}
		return fmt.Errorf("failed to stop container: %v, output: %s", err, string(output))
	}

	config.UpdateContainerStatus(id, "stopped")
	fmt.Printf("Container %d (%s) stopped\n", id, c.Name)
	return nil
}

// RestartContainer restarts an LXC container by its ID
func (m *Manager) RestartContainer(id int) error {
	c := config.FindContainer(id)
	if c == nil {
		return fmt.Errorf("container not found: %d", id)
	}

	status, _ := m.GetContainerStatus(c.LxcName())
	if status == "running" {
		if err := m.StopContainer(id); err != nil {
			if !strings.Contains(err.Error(), "not running") {
				return err
			}
		}
		time.Sleep(1 * time.Second)
	}
	return m.StartContainer(id)
}

// EnsureContainerIPv4 brings eth0 up and asks the guest network stack for DHCP.
func (m *Manager) EnsureContainerIPv4(id int) (string, error) {
	c := config.FindContainer(id)
	if c == nil {
		return "", fmt.Errorf("container not found: %d", id)
	}
	lxcName := c.LxcName()
	status, _ := m.GetContainerStatus(lxcName)
	if status != "running" {
		return "", fmt.Errorf("container %d is not running; cannot configure IPv4", id)
	}

	if ip, err := m.GetContainerIP(lxcName); err == nil && ip != "" {
		c.IP = ip
		config.SaveConfig()
		return ip, nil
	}

	script := `
set +e
ip link set lo up 2>/dev/null
ip link set eth0 up 2>/dev/null

if command -v systemctl >/dev/null 2>&1; then
	systemctl start NetworkManager >/dev/null 2>&1
	systemctl start systemd-networkd >/dev/null 2>&1
	systemctl start networking >/dev/null 2>&1
fi
if command -v rc-service >/dev/null 2>&1; then
	rc-service networking start >/dev/null 2>&1
fi
if command -v nmcli >/dev/null 2>&1; then
	nmcli networking on >/dev/null 2>&1
	nmcli device set eth0 managed yes >/dev/null 2>&1
	nmcli connection up eth0 >/dev/null 2>&1 || nmcli device connect eth0 >/dev/null 2>&1
fi
if command -v dhclient >/dev/null 2>&1; then
	timeout 12 dhclient -4 -v eth0 >/dev/null 2>&1
elif command -v dhcpcd >/dev/null 2>&1; then
	pkill dhcpcd >/dev/null 2>&1 || true
	rm -f /run/dhcpcd*.pid /var/lib/dhcpcd/* /run/dhcpcd/* 2>/dev/null
	timeout 20 dhcpcd -4 -q -w -K eth0 >/dev/null 2>&1
elif command -v udhcpc >/dev/null 2>&1; then
	timeout 12 udhcpc -i eth0 -q >/dev/null 2>&1
elif command -v busybox >/dev/null 2>&1; then
	timeout 12 busybox udhcpc -i eth0 -q >/dev/null 2>&1
fi
ip -4 addr show eth0 2>/dev/null | awk '/inet / {sub(/\/.*/, "", $2); print $2; exit}'
`
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "lxc-attach", "-n", lxcName, "--", "sh", "-c", script)
	output, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return "", fmt.Errorf("timed out waiting for IPv4 DHCP in %s", lxcName)
	}
	if err != nil {
		return "", fmt.Errorf("failed to run IPv4 repair in %s: %v, output: %s", lxcName, err, string(output))
	}
	ip := strings.TrimSpace(string(output))
	if ip == "" {
		return "", fmt.Errorf("no IPv4 address after DHCP repair in %s", lxcName)
	}
	c.IP = ip
	config.SaveConfig()
	return ip, nil
}

// WarmSSH waits briefly for container networking metadata and then ensures sshd
// is installed, configured, running, and using the saved root password.
func (m *Manager) WarmSSH(id int) error {
	c := config.FindContainer(id)
	if c == nil {
		return fmt.Errorf("container not found: %d", id)
	}
	lxcName := c.LxcName()
	status, _ := m.GetContainerStatus(lxcName)
	if status != "running" {
		return fmt.Errorf("container %d is not running; cannot warm SSH", id)
	}

	for retry := 0; retry < 15; retry++ {
		if ip, err := m.GetContainerIP(lxcName); err == nil && ip != "" {
			if current := config.FindContainer(id); current != nil {
				current.IP = ip
				config.SaveConfig()
			}
			break
		}
		time.Sleep(2 * time.Second)
	}
	if current := config.FindContainer(id); current != nil && current.IP == "" {
		if ip, err := m.EnsureContainerIPv4(id); err == nil && ip != "" {
			current.IP = ip
			config.SaveConfig()
		}
	}

	return m.EnsureSSH(id)
}

func (m *Manager) WarmSSHAsync(id int, reason string) {
	if _, loaded := sshWarmupScheduled.LoadOrStore(id, struct{}{}); loaded {
		return
	}
	go func() {
		sshWarmupSem <- struct{}{}
		defer func() {
			<-sshWarmupSem
			sshWarmupScheduled.Delete(id)
		}()

		if err := m.WarmSSH(id); err != nil {
			if c := config.FindContainer(id); c != nil {
				fmt.Printf("Warning: SSH warmup failed for %s (%s, %s): %v\n", c.LxcName(), c.Name, reason, err)
			} else {
				fmt.Printf("Warning: SSH warmup failed for container %d (%s): %v\n", id, reason, err)
			}
		}
	}()
}

func sshEnsureLock(id int) *sync.Mutex {
	lock, _ := sshEnsureLocks.LoadOrStore(id, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// DestroyContainer destroys an LXC container by its ID
func (m *Manager) DestroyContainer(id int) error {
	if id <= 0 {
		return fmt.Errorf("invalid container id: %d", id)
	}
	c := config.FindContainer(id)
	if c == nil {
		return fmt.Errorf("container not found: %d", id)
	}
	lxcName := c.LxcName()
	if c.IPv6 != "" && c.IPv6Interface != "" {
		removeHostIPv6Routing(c.IPv6, c.IPv6Interface)
	}

	if err := m.StopContainer(id); err != nil {
		return fmt.Errorf("failed to stop container before destroy: %v", err)
	}
	time.Sleep(1 * time.Second)

	containerDir := filepath.Join(m.LxcPath, lxcName)
	m.detachContainerMounts(containerDir)
	m.detachContainerLoopDevices(containerDir)

	// Retry lxc-destroy up to 3 times, since LXC may need time to release resources
	var destroyErr error
	for attempt := 0; attempt < 3; attempt++ {
		cmd := exec.Command("lxc-destroy", "-n", lxcName, "-f")
		output, err := cmd.CombinedOutput()
		if err == nil {
			destroyErr = nil
			break
		}
		out := string(output)
		if strings.Contains(strings.ToLower(out), "does not exist") ||
			strings.Contains(strings.ToLower(out), "not found") ||
			strings.Contains(strings.ToLower(out), "is not defined") {
			destroyErr = nil
			break
		}
		destroyErr = fmt.Errorf("failed to destroy container (attempt %d/3): %v, output: %s", attempt+1, err, out)
		if attempt < 2 {
			// Wait for LXC to release resources before retry
			time.Sleep(2 * time.Second)
			m.detachContainerMounts(containerDir)
			m.detachContainerLoopDevices(containerDir)
		}
	}

	if err := m.cleanupContainerStorage(lxcName); err != nil {
		if destroyErr != nil {
			return fmt.Errorf("%v; cleanup also failed: %v", destroyErr, err)
		}
		return err
	}
	if status, err := m.GetContainerStatus(lxcName); err == nil {
		if destroyErr != nil {
			return fmt.Errorf("%v; container still exists after cleanup with status %s", destroyErr, status)
		}
		return fmt.Errorf("container still exists after cleanup with status %s", status)
	}
	// Remove snapshot physical files (by container ID, not lxcName)
	snapshotDir := filepath.Join(snapshotBaseDir(), strconv.Itoa(id))
	if err := safePathUnder(snapshotDir, snapshotBaseDir()); err == nil {
		os.RemoveAll(snapshotDir)
	}

	// Also remove any legacy snapshot dir that used lxcName
	legacySnapshotDir := filepath.Join(snapshotBaseDir(), lxcName)
	if legacySnapshotDir != snapshotDir {
		if err := safePathUnder(legacySnapshotDir, snapshotBaseDir()); err == nil {
			os.RemoveAll(legacySnapshotDir)
		}
	}

	if !config.RemoveContainer(id) {
		return fmt.Errorf("container destroyed but config entry was not removed: %d", id)
	}
	if config.FindContainer(id) != nil {
		return fmt.Errorf("container destroyed but config entry still exists: %d", id)
	}
	fmt.Printf("Container %d destroyed\n", id)
	return nil
}

// EnsureSSH installs and starts sshd, enables root password login, and verifies port 22.
func (m *Manager) EnsureSSH(id int) error {
	lock := sshEnsureLock(id)
	lock.Lock()
	defer lock.Unlock()

	c := config.FindContainer(id)
	if c == nil {
		return fmt.Errorf("container not found: %d", id)
	}
	lxcName := c.LxcName()

	status, _ := m.GetContainerStatus(lxcName)
	if status != "running" {
		return fmt.Errorf("container %d is not running; cannot configure SSH", id)
	}

	if c.SSHPassword == "" {
		c.SSHPassword = generateRandomString(16)
		config.SaveConfig()
	}

	script := sshSetupScript(c.SSHPassword, true)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "lxc-attach", "-n", lxcName, "--", "sh", "-c", script)
	output, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("timed out configuring SSH in container %d after 180s; package manager or service startup may be stuck, output: %s", id, string(output))
	}
	if err != nil {
		return fmt.Errorf("failed to configure SSH in container %d: %v, output: %s", id, err, string(output))
	}

	if c.IP == "" {
		if ip, ipErr := m.GetContainerIP(lxcName); ipErr == nil && ip != "" {
			c.IP = ip
			config.SaveConfig()
		}
	}
	if c.IP != "" {
		if mapErr := m.ApplyPortMappings(id); mapErr != nil {
			fmt.Printf("Warning: failed to refresh SSH port mapping for %s: %v\n", lxcName, mapErr)
		}
	}

	fmt.Printf("SSH ready in container %d (root password login enabled)\n", id)
	return nil
}

func (m *Manager) quickEnsureSSHPassword(lxcName, password string) error {
	if password == "" {
		return fmt.Errorf("empty SSH password")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "lxc-attach", "-n", lxcName, "--", "sh", "-c",
		fmt.Sprintf("printf '%%s:%%s\\n' root %s | chpasswd", shellQuote(password)))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to update SSH password quickly: %v, output: %s", err, string(output))
	}
	return nil
}

func (m *Manager) containerPortListening(lxcName string, port int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	check := fmt.Sprintf("(ss -ltn 2>/dev/null || netstat -tln 2>/dev/null) | grep -Eq '(^|[[:space:]])[^[:space:]]*:%d[[:space:]]'", port)
	return exec.CommandContext(ctx, "lxc-attach", "-n", lxcName, "--", "sh", "-c", check).Run() == nil
}

func sshSetupScript(password string, startService bool) string {
	script := `set -u
ROOT_PASSWORD=` + shellQuote(password) + `

# DNS setup: handle both traditional /etc/resolv.conf and systemd-resolved (Ubuntu 24.04).
# On modern distros, /etc/resolv.conf is a symlink managed by systemd-resolved.
# Remove any existing symlink and write a real file so DNS always works in LXC.
if [ -L /etc/resolv.conf ] 2>/dev/null; then
	rm -f /etc/resolv.conf
fi
# Also try resolvectl for systemd-resolved setups
if command -v resolvectl >/dev/null 2>&1; then
	resolvectl dns eth0 10.0.3.1 2>/dev/null || true
	resolvectl dns eth0 8.8.8.8 2>/dev/null || true
	resolvectl domain eth0 '~.' 2>/dev/null || true
fi
# Check if we have a usable (non-localhost) nameserver; if not, force-write one.
# Avoid the trap where systemd stub resolver puts "nameserver 127.0.0.53"
# but doesn't actually resolve anything.
if ! grep -q '^nameserver [1-9]' /etc/resolv.conf 2>/dev/null; then
	echo "nameserver 10.0.3.1" > /etc/resolv.conf
	echo "nameserver 8.8.8.8" >> /etc/resolv.conf
fi
export DEBIAN_FRONTEND=noninteractive
export APT_LISTCHANGES_FRONTEND=none

run_timeout() {
	if command -v timeout >/dev/null 2>&1; then
		timeout "$@"
	else
		shift
		"$@"
	fi
}

has_sshd() {
	command -v sshd >/dev/null 2>&1 || [ -x /usr/sbin/sshd ] || [ -x /sbin/sshd ]
}

sshd_path() {
	if command -v sshd >/dev/null 2>&1; then
		command -v sshd
	elif [ -x /usr/sbin/sshd ]; then
		printf /usr/sbin/sshd
	elif [ -x /sbin/sshd ]; then
		printf /sbin/sshd
	else
		return 1
	fi
}

install_sshd() {
	if has_sshd; then
		return 0
	fi
	if command -v apt-get >/dev/null 2>&1; then
		for i in 1 2; do
			run_timeout 20 dpkg --configure -a >/dev/null 2>&1 || true
			run_timeout 45 apt-get update -o Acquire::Retries=2 -o Acquire::http::Timeout=15 &&
				run_timeout 90 apt-get install -y --no-install-recommends -o Dpkg::Options::=--force-confold openssh-server passwd iproute2 procps net-tools &&
				return 0
			sleep 3
		done
	elif command -v apk >/dev/null 2>&1; then
		for i in 1 2 3; do
			run_timeout 120 apk add --no-cache openssh-server openssh-client shadow iproute2 procps net-tools && return 0
			sleep 3
		done
	elif command -v pacman >/dev/null 2>&1; then
		run_timeout 45 pacman -Syu --noconfirm >/dev/null 2>&1 || true
		run_timeout 90 pacman -S --noconfirm openssh shadow iproute2 procps-ng net-tools && return 0
	elif command -v dnf >/dev/null 2>&1; then
		run_timeout 90 dnf install -y openssh-server openssh-clients passwd iproute procps-ng net-tools && return 0
	elif command -v yum >/dev/null 2>&1; then
		run_timeout 90 yum install -y openssh-server openssh-clients passwd iproute procps-ng net-tools && return 0
	elif command -v nix-env >/dev/null 2>&1; then
		{ run_timeout 120 nix-env -iA nixos.openssh >/dev/null 2>&1 && return 0; } || true
		# NixOS cloud images may already have sshd or use different package paths
		# Fall through to let the script try whatever sshd is available
	fi
	return 1
}

set_sshd_option() {
	key="$1"
	value="$2"
	file=/etc/ssh/sshd_config
	tmp="${file}.clicd"
	touch "$file"
	awk -v key="$key" -v value="$value" '
		BEGIN { done=0; inmatch=0 }
		/^[[:space:]]*Match[[:space:]]/ {
			if (!done) { print key " " value; done=1 }
			inmatch=1
			print
			next
		}
		!inmatch && $0 ~ "^[#[:space:]]*" key "[[:space:]]+" {
			if (!done) { print key " " value; done=1 }
			next
		}
		{ print }
		END { if (!done) print key " " value }
	' "$file" >"$tmp" && cat "$tmp" >"$file"
	rm -f "$tmp"
}

install_sshd || exit 30

mkdir -p /run/sshd /var/run/sshd /etc/ssh /etc/ssh/sshd_config.d
ssh-keygen -A >/dev/null 2>&1 || true

cat >/etc/ssh/sshd_config.d/99-clicd.conf <<'EOF'
PermitRootLogin yes
PasswordAuthentication yes
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
UsePAM no
EOF

set_sshd_option PermitRootLogin yes
set_sshd_option PasswordAuthentication yes
set_sshd_option KbdInteractiveAuthentication no
set_sshd_option ChallengeResponseAuthentication no
set_sshd_option UsePAM no

if [ -n "$ROOT_PASSWORD" ]; then
	printf '%s:%s\n' root "$ROOT_PASSWORD" | chpasswd || exit 31
	passwd -u root >/dev/null 2>&1 || true
fi

if command -v rc-update >/dev/null 2>&1; then
	rc-update add sshd default >/dev/null 2>&1 || true
fi
if command -v systemctl >/dev/null 2>&1; then
	systemctl stop ssh.socket 2>/dev/null || true
	systemctl disable ssh.socket 2>/dev/null || true
	systemctl enable ssh >/dev/null 2>&1 || systemctl enable sshd >/dev/null 2>&1 || true
fi
if command -v update-rc.d >/dev/null 2>&1; then
	update-rc.d ssh defaults >/dev/null 2>&1 || true
fi
if command -v chkconfig >/dev/null 2>&1; then
	chkconfig sshd on >/dev/null 2>&1 || true
fi

SSHD_BIN="$(sshd_path)" || exit 32
"$SSHD_BIN" -t -f /etc/ssh/sshd_config >/tmp/clicd-sshd-test.log 2>&1 || {
	cat /tmp/clicd-sshd-test.log
	exit 32
}
`
	if !startService {
		return script
	}
	return script + `
# Disable socket-activated SSH (Ubuntu 24.04 default) to avoid conflicts.
if command -v systemctl >/dev/null 2>&1; then
	systemctl stop ssh.socket 2>/dev/null || true
	systemctl disable ssh.socket 2>/dev/null || true
fi
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
	systemctl restart ssh >/dev/null 2>&1 || systemctl restart sshd >/dev/null 2>&1 || true
fi
service ssh restart >/dev/null 2>&1 ||
	service sshd restart >/dev/null 2>&1 ||
	rc-service sshd restart >/dev/null 2>&1 ||
	/etc/init.d/ssh restart >/dev/null 2>&1 ||
	/etc/init.d/sshd restart >/dev/null 2>&1 ||
	true

if ! (ss -ltn 2>/dev/null || netstat -tln 2>/dev/null) | grep -Eq '(^|[[:space:]])[^[:space:]]*:22[[:space:]]'; then
	pkill -x sshd >/dev/null 2>&1 || killall sshd >/dev/null 2>&1 || true
	rm -f /run/sshd.pid /var/run/sshd.pid
	"$SSHD_BIN" -f /etc/ssh/sshd_config >/dev/null 2>&1 || exit 32
fi

for i in 1 2 3 4 5; do
	if (ss -ltn 2>/dev/null || netstat -tln 2>/dev/null) | grep -Eq '(^|[[:space:]])[^[:space:]]*:22[[:space:]]'; then
		exit 0
	fi
	sleep 1
done
pgrep -x sshd >/dev/null 2>&1 || exit 33
`
}

// ResetSSHPassword resets the root password of a container
func (m *Manager) ResetSSHPassword(id int) (string, error) {
	c := config.FindContainer(id)
	if c == nil {
		return "", fmt.Errorf("container not found: %d", id)
	}
	lxcName := c.LxcName()

	newPassword := generateRandomString(16)

	if c.Status == "running" {
		c.SSHPassword = newPassword
		config.SaveConfig()
		if err := m.EnsureSSH(id); err != nil {
			return "", err
		}
	} else {
		if err := m.ensureDiskImageMounted(lxcName); err != nil {
			return "", err
		}
		rootfsPath := filepath.Join(m.LxcPath, lxcName, "rootfs")
		if err := m.preconfigureSSH(rootfsPath, newPassword, c.Template); err != nil {
			return "", fmt.Errorf("failed to configure SSH: %v", err)
		}
		cmd := m.rootfsCommand(rootfsPath, "sh", "-c", fmt.Sprintf("printf '%%s:%%s\\n' root %s | chpasswd", shellQuote(newPassword)))
		output, err := cmd.CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("failed to set password: %v, output: %s", err, string(output))
		}
		c.SSHPassword = newPassword
		config.SaveConfig()
	}

	return newPassword, nil
}

func (m *Manager) rootfsCommand(rootfsPath string, args ...string) *exec.Cmd {
	marker := filepath.Join(rootfsPath, ".clicd-unprivileged-shifted")
	if _, err := os.Stat(marker); err == nil {
		uidBase, gidBase, mapErr := unprivilegedIDMap()
		if mapErr == nil {
			cmdArgs := []string{
				"-m", fmt.Sprintf("u:0:%d:65536", uidBase),
				"-m", fmt.Sprintf("g:0:%d:65536", gidBase),
				"--", "chroot", rootfsPath,
			}
			cmdArgs = append(cmdArgs, args...)
			return exec.Command("lxc-usernsexec", cmdArgs...)
		}
	}
	cmdArgs := append([]string{rootfsPath}, args...)
	return exec.Command("chroot", cmdArgs...)
}

func (m *Manager) cleanupContainerStorage(lxcName string) error {
	containerDir := filepath.Join(m.LxcPath, lxcName)
	cleanPath, err := filepath.Abs(containerDir)
	if err != nil {
		return fmt.Errorf("failed to resolve container path: %v", err)
	}
	basePath, err := filepath.Abs(m.LxcPath)
	if err != nil {
		return fmt.Errorf("failed to resolve LXC path: %v", err)
	}
	if cleanPath == basePath || !strings.HasPrefix(cleanPath, basePath+string(os.PathSeparator)) {
		return fmt.Errorf("refusing to remove unsafe container path: %s", cleanPath)
	}
	if _, err := os.Stat(cleanPath); os.IsNotExist(err) {
		return nil
	}
	exec.Command("lxc-stop", "-n", lxcName, "-k").Run()
	exec.Command("lxc-destroy", "-n", lxcName, "-f").Run()
	m.detachContainerMounts(cleanPath)
	m.detachContainerLoopDevices(cleanPath)
	rootfs := filepath.Join(cleanPath, "rootfs")
	exec.Command("umount", "-R", "-l", rootfs).Run()
	m.detachContainerMounts(cleanPath)
	m.detachContainerLoopDevices(cleanPath)
	if err := os.RemoveAll(cleanPath); err != nil {
		return fmt.Errorf("failed to remove container directory %s: %v", cleanPath, err)
	}
	return nil
}

func (m *Manager) detachContainerMounts(containerDir string) {
	out, err := exec.Command("findmnt", "-R", "-n", "-o", "TARGET", containerDir).Output()
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
		lines[i], lines[j] = lines[j], lines[i]
	}
	for _, line := range lines {
		target := strings.TrimSpace(line)
		if target == "" {
			continue
		}
		exec.Command("umount", "-R", "-l", target).Run()
	}
}

func (m *Manager) detachContainerLoopDevices(containerDir string) {
	out, err := exec.Command("losetup", "-j", filepath.Join(containerDir, "rootfs.img")).Output()
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		device := strings.TrimSuffix(strings.SplitN(line, ":", 2)[0], ":")
		if device != "" {
			exec.Command("losetup", "-d", device).Run()
		}
	}
}

// GetContainerStatus gets container running status
func (m *Manager) GetContainerStatus(lxcName string) (string, error) {
	cmd := exec.Command("lxc-info", "-n", lxcName, "-sH")
	output, err := cmd.Output()
	if err != nil {
		cmd2 := exec.Command("lxc-info", "-n", lxcName, "-s")
		output2, err2 := cmd2.Output()
		if err2 != nil {
			return "unknown", err2
		}
		output = output2
	}

	status := strings.TrimSpace(string(output))
	upper := strings.ToUpper(status)
	if strings.Contains(upper, "RUNNING") {
		return "running", nil
	}
	return "stopped", nil
}

// GetContainerIP gets container IP address
func (m *Manager) GetContainerIP(lxcName string) (string, error) {
	cmd := exec.Command("lxc-info", "-n", lxcName, "-iH")
	output, err := cmd.Output()
	if err != nil {
		cmd2 := exec.Command("lxc-info", "-n", lxcName, "-i")
		output2, err2 := cmd2.Output()
		if err2 != nil {
			return "", err2
		}
		output = output2
	}

	ip := strings.TrimSpace(string(output))
	// Always prefer IPv4; IPv6 addresses break WebSSH and port forwarding.
	re := regexp.MustCompile(`(\d+\.\d+\.\d+\.\d+)`)
	matches := re.FindStringSubmatch(ip)
	if len(matches) > 1 {
		return matches[1], nil
	}
	// If no IPv4 found, try lxc-attach as fallback (DHCP may be delayed)
	attachCmd := exec.Command("lxc-attach", "-n", lxcName, "--", "sh", "-c", "ip -4 addr show eth0 2>/dev/null | grep -oP 'inet \\K[\\d.]+' || true")
	if attachOut, attachErr := attachCmd.Output(); attachErr == nil {
		v4 := strings.TrimSpace(string(attachOut))
		if v4 != "" {
			return v4, nil
		}
	}
	return "", fmt.Errorf("no IPv4 address found for %s (IPv6 is disabled for containers)", lxcName)
}

// ListContainers lists all LXC containers and updates statuses
func (m *Manager) ListContainers() ([]config.Container, error) {
	containers := config.AppConfig.Containers
	for i := range containers {
		if containers[i].IsKVM() {
			continue
		}
		status, err := m.GetContainerStatus(containers[i].LxcName())
		if err == nil {
			containers[i].Status = status
		}
		if status == "running" {
			ip, err := m.GetContainerIP(containers[i].LxcName())
			if err == nil {
				containers[i].IP = ip
			}
		}
	}
	return containers, nil
}

// ImportExistingClicdContainers imports existing LXC containers into the CLICD
// config. Native CLICD containers keep ct-{id}; arbitrary LXC names are stored
// in Container.LXCName so Web and CLI can manage the same imported container.
func (m *Manager) ImportExistingClicdContainers() ([]config.Container, error) {
	entries, err := os.ReadDir(m.LxcPath)
	if err != nil {
		return nil, err
	}

	existingIDs := make(map[int]bool)
	existingNames := make(map[string]bool)
	existingLXCNames := make(map[string]bool)
	maxID := config.AppConfig.NextContainerID - 1
	for _, c := range config.AppConfig.Containers {
		existingIDs[c.ID] = true
		existingNames[c.Name] = true
		existingLXCNames[c.LxcName()] = true
		if c.ID > maxID {
			maxID = c.ID
		}
	}

	re := regexp.MustCompile(`^ct-([0-9]+)$`)
	imported := make([]config.Container, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		lxcName := entry.Name()
		if existingLXCNames[lxcName] {
			continue
		}

		id := 0
		if matches := re.FindStringSubmatch(lxcName); len(matches) == 2 {
			if parsed, err := strconv.Atoi(matches[1]); err == nil && parsed > 0 && !existingIDs[parsed] {
				id = parsed
			}
		}
		if id == 0 {
			id = maxID + 1
			for existingIDs[id] {
				id++
			}
		}

		name := lxcName
		if existingNames[name] {
			name = fmt.Sprintf("imported-%d", id)
		}

		status, err := m.GetContainerStatus(lxcName)
		if err != nil || status == "" {
			status = "unknown"
		}

		c := config.Container{
			ID:               id,
			UUID:             config.NewContainerUUID(),
			Name:             name,
			LXCName:          lxcName,
			Virtualization:   config.VirtualizationLXC,
			Template:         "imported",
			VCPU:             1,
			RAMMB:            512,
			DiskGB:           10,
			NetworkBWMbps:    100,
			MonthlyTrafficGB: 1000,
			TrafficMode:      "total",
			Status:           status,
			CreatedAt:        time.Now().Format(time.RFC3339),
			PortMappingLimit: 2,
			SnapshotLimit:    config.DefaultSnapshotLimit,
		}

		if status == "running" {
			if ip, err := m.GetContainerIP(lxcName); err == nil {
				c.IP = ip
			}
		}

		config.AppConfig.Containers = append(config.AppConfig.Containers, c)
		imported = append(imported, c)
		existingIDs[id] = true
		existingNames[name] = true
		existingLXCNames[lxcName] = true
		if id > maxID {
			maxID = id
		}
	}

	if len(imported) > 0 {
		config.AppConfig.NextContainerID = maxID + 1
		if err := config.SaveConfig(); err != nil {
			return nil, err
		}
	}

	return imported, nil
}

// ReinstallContainer reinstalls the container OS
func (m *Manager) ReinstallContainer(id int, templateID string) error {
	c := config.FindContainer(id)
	if c == nil {
		return fmt.Errorf("container not found: %d", id)
	}

	tmpl := FindTemplate(templateID)
	if tmpl == nil {
		return fmt.Errorf("template not found: %s", templateID)
	}

	lxcName := c.LxcName()

	// Stop the container first
	status, _ := m.GetContainerStatus(lxcName)
	if status == "running" {
		m.StopContainer(id)
	}

	// Clean port mappings temporarily
	m.CleanPortMappings(id)

	// Destroy old LXC but keep config
	exec.Command("lxc-stop", "-n", lxcName, "-k").Run()
	exec.Command("lxc-destroy", "-n", lxcName, "-f").Run()
	rootfs := filepath.Join(m.LxcPath, lxcName, "rootfs")
	exec.Command("umount", "-R", "-l", rootfs).Run()
	os.RemoveAll(rootfs)
	os.Remove(filepath.Join(m.LxcPath, lxcName, "rootfs.img"))

	// Create new container with same LXC name (preserves ID)
	cmd := exec.Command("lxc-create",
		"-n", lxcName,
		"-t", "download",
		"--",
		"-d", tmpl.Distro,
		"-r", tmpl.Release,
		"-a", tmpl.Arch,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("lxc-create failed: %v, output: %s", err, string(output))
	}

	if err := m.applyDiskLimit(lxcName, c.DiskGB); err != nil {
		return err
	}

	// Re-apply resource limits and mandatory security hardening.
	cfg := ContainerConfig{
		Name:             c.Name,
		TemplateID:       templateID,
		VCPU:             c.VCPU,
		RAMMB:            c.RAMMB,
		DiskGB:           c.DiskGB,
		NetworkBWMbps:    c.NetworkBWMbps,
		MonthlyTrafficGB: c.MonthlyTrafficGB,
		IOSpeedMBps:      c.IOSpeedMBps,
		AssignIPv6:       c.IPv6 != "",
		ExpiresAt:        c.ExpiresAt,
	}
	if err := m.applyResourceLimits(lxcName, cfg); err != nil {
		return err
	}
	if c.IPv6 != "" {
		if err := m.applyIPv6Config(lxcName, c.IPv6); err != nil {
			return err
		}
	}

	// Set root password and pre-configure network/SSH via chroot.
	rootfsPath := filepath.Join(m.LxcPath, lxcName, "rootfs")
	m.preconfigureNetwork(rootfsPath, templateID)
	if c.IPv6 != "" {
		if err := installContainerIPv6Init(rootfsPath, c.IPv6); err != nil {
			fmt.Printf("Warning: failed to install IPv6 init in %s after reinstall: %v\n", lxcName, err)
		}
	}
	if c.SSHPassword == "" {
		c.SSHPassword = generateRandomString(16)
	}
	if err := m.preconfigureSSH(rootfsPath, c.SSHPassword, templateID); err != nil {
		fmt.Printf("Warning: failed to pre-configure SSH in %s after reinstall: %v\n", lxcName, err)
	}
	if err := m.shiftRootfsForUnprivileged(lxcName); err != nil {
		return err
	}
	setCmd := m.rootfsCommand(rootfsPath,
		"sh", "-c", fmt.Sprintf("printf '%%s:%%s\\n' root %s | chpasswd", shellQuote(c.SSHPassword)))
	setCmd.Run()

	// Update template and keep everything else the same
	c.Template = templateID
	c.SSHHostKey = ""
	c.Status = "running"
	config.SaveConfig()

	// Start the container to trigger ensureSSH
	if err := m.ensureDiskImageMounted(lxcName); err != nil {
		c.Status = "stopped"
		config.SaveConfig()
		return err
	}
	logFile := filepath.Join(os.TempDir(), "clicd-"+lxcName+"-start.log")
	os.Remove(logFile)
	startCmd := exec.Command("lxc-start", "-n", lxcName, "-d", "--logfile", logFile, "--logpriority", "DEBUG")
	if output, err := startCmd.CombinedOutput(); err != nil {
		fmt.Printf("Warning: failed to start container after reinstall: %v\n", err)
		c.Status = "stopped"
		config.SaveConfig()
		return fmt.Errorf("reinstalled but failed to start: %v, output: %s, lxc log: %s", err, string(output), tailFile(logFile, 80))
	}

	// Wait for network and install SSH
	time.Sleep(5 * time.Second)
	var ip string
	for retry := 0; retry < 5; retry++ {
		ip, _ = m.GetContainerIP(lxcName)
		if ip != "" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if ip != "" {
		c.IP = ip
		config.SaveConfig()
	}
	if ip != "" {
		if err := m.EnsureSSH(id); err != nil {
			return err
		}
	}
	// Apply bandwidth limit after reinstall
	if c.NetworkBWMbps > 0 {
		m.applyBandwidthLimit(c.LxcName(), c.NetworkBWMbps)
	}
	if c.IPv6 != "" {
		if err := m.ApplyIPv6(id); err != nil {
			fmt.Printf("Warning: failed to apply IPv6 after reinstall: %v\n", err)
		}
	}

	fmt.Printf("Container %d (%s) reinstalled with %s\n", id, c.Name, templateID)
	return nil
}

// GetResourceUsage returns resource usage info for a container by ID.
// Rates come from the background monitor goroutine (no shared-state races).
func (m *Manager) GetResourceUsage(id int) (map[string]interface{}, error) {
	c := config.FindContainer(id)
	if c == nil {
		return nil, fmt.Errorf("container not found: %d", id)
	}
	lxcName := c.LxcName()
	usage := make(map[string]interface{})

	// Read raw values
	load1, load5, load15 := m.getContainerLoadAvg(lxcName)
	usage["load1"] = load1
	usage["load5"] = load5
	usage["load15"] = load15

	memUsage := readIntCommand(fmt.Sprintf(
		"cat /sys/fs/cgroup/lxc/%[1]s/memory.current 2>/dev/null || "+
			"cat /sys/fs/cgroup/lxc.payload.%[1]s/memory.current 2>/dev/null || "+
			"cat /sys/fs/cgroup/memory/lxc/%[1]s/memory.usage_in_bytes 2>/dev/null || echo 0", shellQuote(lxcName)))
	usage["memory_usage_bytes"] = memUsage

	cpuUsec := uint64(readIntCommand(fmt.Sprintf(
		"(cat /sys/fs/cgroup/lxc/%[1]s/cpu.stat 2>/dev/null || "+
			"cat /sys/fs/cgroup/lxc.payload.%[1]s/cpu.stat 2>/dev/null) | "+
			"awk '/usage_usec/ {print $2; found=1} END {if (!found) print 0}'", shellQuote(lxcName))))
	usage["cpu_usage_usec"] = cpuUsec

	diskUsage := readIntCommand(fmt.Sprintf("du -s -B1 %s 2>/dev/null | awk '{print $1}' || echo 0",
		shellQuote(filepath.Join(m.LxcPath, lxcName, "rootfs"))))
	usage["disk_usage_bytes"] = diskUsage

	rxBytes, txBytes := m.getContainerNetworkBytes(lxcName)
	usage["network_rx_bytes"] = rxBytes
	usage["network_tx_bytes"] = txBytes

	readBytes, writeBytes := m.getContainerDiskIOBytes(lxcName)
	usage["disk_read_bytes"] = readBytes
	usage["disk_write_bytes"] = writeBytes

	// Read rates from background monitor cache (single writer, no race)
	usageMu.RLock()
	rate, hasRate := rateCache[lxcName]
	usageMu.RUnlock()

	if hasRate && time.Since(rate.UpdatedAt) < 15*time.Second {
		usage["cpu_usage_pct"] = rate.CPUPct
		usage["network_rx_bps"] = rate.RXBps
		usage["network_tx_bps"] = rate.TXBps
		usage["disk_read_bps"] = rate.ReadBps
		usage["disk_write_bps"] = rate.WriteBps
	} else {
		// Cache miss or stale — return zeros (monitor will populate soon)
		usage["cpu_usage_pct"] = 0.0
		usage["network_rx_bps"] = 0.0
		usage["network_tx_bps"] = 0.0
		usage["disk_read_bps"] = 0.0
		usage["disk_write_bps"] = 0.0
	}

	return usage, nil
}

func (m *Manager) getContainerLoadAvg(lxcName string) (float64, float64, float64) {
	pid := m.getContainerInitPID(lxcName)
	if pid == "" {
		return 0, 0, 0
	}
	out, err := exec.Command("nsenter", "-t", pid, "-m", "-p", "cat", "/proc/loadavg").Output()
	if err != nil {
		return 0, 0, 0
	}
	parts := strings.Fields(string(out))
	if len(parts) < 3 {
		return 0, 0, 0
	}
	load1, _ := strconv.ParseFloat(parts[0], 64)
	load5, _ := strconv.ParseFloat(parts[1], 64)
	load15, _ := strconv.ParseFloat(parts[2], 64)
	return load1, load5, load15
}

func (m *Manager) getContainerNetworkBytes(lxcName string) (uint64, uint64) {
	pid := m.getContainerInitPID(lxcName)
	if pid == "" {
		return 0, 0
	}
	return readProcNetDev(fmt.Sprintf("/proc/%s/net/dev", pid))
}

// readProcNetDev parses /proc/PID/net/dev directly (no shell/awk fork).
func readProcNetDev(path string) (uint64, uint64) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0
	}
	var rx, tx uint64
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		// Skip header lines
		if strings.Contains(line, "|") || strings.Contains(line, "face") || strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Fields(line)
		// Fields: face | rx_bytes rx_packets rx_errs rx_drop rx_fifo rx_frame rx_compressed rx_multicast | tx_bytes tx_packets tx_errs tx_drop tx_fifo tx_colls tx_carrier tx_compressed
		// Skip loopback (face starts with "lo")
		if len(fields) < 10 {
			continue
		}
		if strings.HasPrefix(fields[0], "lo") {
			continue
		}
		r, _ := strconv.ParseUint(fields[1], 10, 64)
		t, _ := strconv.ParseUint(fields[9], 10, 64)
		rx += r
		tx += t
	}
	return rx, tx
}

func (m *Manager) getContainerDiskIOBytes(lxcName string) (uint64, uint64) {
	pid := m.getContainerInitPID(lxcName)
	if pid == "" {
		return 0, 0
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%s/io", pid))
	if err != nil {
		return 0, 0
	}
	var readBytes, writeBytes uint64
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "read_bytes:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "read_bytes:"))
			readBytes, _ = strconv.ParseUint(val, 10, 64)
		} else if strings.HasPrefix(line, "write_bytes:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "write_bytes:"))
			writeBytes, _ = strconv.ParseUint(val, 10, 64)
		}
	}
	return readBytes, writeBytes
}

func (m *Manager) getContainerInitPID(lxcName string) string {
	cmd := exec.Command("lxc-info", "-n", lxcName, "-pH")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// readCgroupFile tries each path template in order, reads the file directly (no shell),
// and returns the first valid int64 value.
func readCgroupFile(name string, paths ...string) int64 {
	for _, tmpl := range paths {
		data, err := os.ReadFile(fmt.Sprintf(tmpl, name))
		if err != nil {
			continue
		}
		val, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if err == nil && val > 0 {
			return val
		}
	}
	return 0
}

// readCgroupCPUUsec tries each path template, reads cpu.stat, and extracts usage_usec.
func readCgroupCPUUsec(name string, paths ...string) uint64 {
	for _, tmpl := range paths {
		data, err := os.ReadFile(fmt.Sprintf(tmpl, name))
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "usage_usec ") {
				val, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "usage_usec")), 10, 64)
				if err == nil {
					return val
				}
			}
		}
	}
	return 0
}

// getNetworkBytesForPID reads /proc/PID/net/dev for a given PID (no lxc-info needed).
func getNetworkBytesForPID(pid string) (uint64, uint64) {
	if pid == "" {
		return 0, 0
	}
	return readProcNetDev(fmt.Sprintf("/proc/%s/net/dev", pid))
}

// getDiskIOBytesForPID reads /proc/PID/io for a given PID (no lxc-info needed).
func getDiskIOBytesForPID(pid string) (uint64, uint64) {
	if pid == "" {
		return 0, 0
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%s/io", pid))
	if err != nil {
		return 0, 0
	}
	var readBytes, writeBytes uint64
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "read_bytes:") {
			readBytes, _ = strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "read_bytes:")), 10, 64)
		} else if strings.HasPrefix(line, "write_bytes:") {
			writeBytes, _ = strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "write_bytes:")), 10, 64)
		}
	}
	return readBytes, writeBytes
}

// getContainerUptimeSeconds returns how long the container has been running (in seconds).
func (m *Manager) getContainerUptimeSeconds(lxcName string) float64 {
	pid := m.getContainerInitPID(lxcName)
	if pid == "" {
		return 0
	}
	// Read process starttime from /proc/PID/stat (field 22 after the closing paren)
	statContent := readFile(fmt.Sprintf("/proc/%s/stat", pid))
	if statContent == "" {
		return 0
	}
	// Find the closing paren of comm field, then we need field 22 after that
	parenIdx := strings.LastIndex(statContent, ")")
	if parenIdx < 0 {
		return 0
	}
	fields := strings.Fields(statContent[parenIdx+2:])
	if len(fields) < 20 {
		return 0
	}
	// starttime is the 20th field after the comm (since state=0, ppid=1, ...)
	starttime := fields[19]
	ticks, err := strconv.ParseInt(starttime, 10, 64)
	if err != nil {
		return 0
	}
	// Get system uptime
	uptimeContent := readFile("/proc/uptime")
	if uptimeContent == "" {
		return 0
	}
	uptimeParts := strings.Fields(uptimeContent)
	if len(uptimeParts) < 1 {
		return 0
	}
	systemUptime, err := strconv.ParseFloat(uptimeParts[0], 64)
	if err != nil {
		return 0
	}
	// Get clock ticks per second (usually 100)
	clkTck := int64(100)
	clkTckContent := readFile("/proc/stat")
	if clkTckContent != "" {
		for _, line := range strings.Split(clkTckContent, "\n") {
			if strings.HasPrefix(line, "btime ") {
				// Can use this to double-check but not needed
				break
			}
		}
	}
	processUptime := float64(ticks) / float64(clkTck)
	containerUptime := systemUptime - processUptime
	if containerUptime < 0 {
		containerUptime = 0
	}
	return containerUptime
}

func readFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

func tailFile(path string, maxLines int) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return strings.Join(lines, "\n")
}

// GetContainerMemoryCgroupUsage returns memory.usage_in_bytes for a container, for usage-only queries.
func (m *Manager) GetContainerMemoryCgroupUsage(id int) int64 {
	c := config.FindContainer(id)
	if c == nil {
		return 0
	}
	return readIntCommand(fmt.Sprintf(
		"cat /sys/fs/cgroup/lxc/%[1]s/memory.current 2>/dev/null || "+
			"cat /sys/fs/cgroup/lxc.payload.%[1]s/memory.current 2>/dev/null || "+
			"cat /sys/fs/cgroup/memory/lxc/%[1]s/memory.usage_in_bytes 2>/dev/null || echo 0", shellQuote(c.LxcName())))
}

func shellQuote(s string) string {
	return fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "'\\''"))
}

func readIntCommand(cmdStr string) int64 {
	cmd := exec.Command("sh", "-c", cmdStr)
	out, err := cmd.Output()
	if err != nil {
		return 0
	}
	val, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return val
}

func generateRandomString(length int) string {
	b := make([]byte, length)
	rand.Read(b)
	return hex.EncodeToString(b)[:length]
}

// lastTrafficSnapshot stores previous network byte counts for delta calculation
var (
	lastTrafficSnapshot   = map[string]trafficSample{}
	lastTrafficSnapshotMu sync.Mutex
)

type trafficSample struct {
	RXBytes uint64
	TXBytes uint64
}

// AccumulateTraffic tracks container network traffic usage (delta-based, called periodically)
func (m *Manager) AccumulateTraffic() {
	currentMonth := time.Now().Format("2006-01")
	lastTrafficSnapshotMu.Lock()
	defer lastTrafficSnapshotMu.Unlock()

	changed := false
	for i := range config.AppConfig.Containers {
		c := &config.AppConfig.Containers[i]
		if c.Status != "running" {
			// Remove snapshot for stopped containers
			delete(lastTrafficSnapshot, c.LxcName())
			continue
		}
		if c.IsKVM() {
			continue
		}
		// Reset if new month
		if c.TrafficResetDate != currentMonth {
			c.TrafficUsedRX = 0
			c.TrafficUsedTX = 0
			c.TrafficResetDate = currentMonth
			delete(lastTrafficSnapshot, c.LxcName())
			changed = true
		}
		rx, tx := m.getContainerNetworkBytes(c.LxcName())
		prev, exists := lastTrafficSnapshot[c.LxcName()]
		// Only add the DELTA (increment since last snapshot)
		if exists && rx >= prev.RXBytes && tx >= prev.TXBytes {
			deltaRX := int64(rx - prev.RXBytes)
			deltaTX := int64(tx - prev.TXBytes)
			if deltaRX > 0 || deltaTX > 0 {
				c.TrafficUsedRX += deltaRX
				c.TrafficUsedTX += deltaTX
				changed = true
			}
		}
		lastTrafficSnapshot[c.LxcName()] = trafficSample{RXBytes: rx, TXBytes: tx}
	}
	if changed {
		config.SaveConfig()
	}
}

// GetTrafficInfo returns traffic usage info for a container
func (m *Manager) GetTrafficInfo(id int) map[string]interface{} {
	c := config.FindContainer(id)
	if c == nil {
		return nil
	}
	currentMonth := time.Now().Format("2006-01")
	if c.TrafficResetDate != currentMonth {
		c.TrafficUsedRX = 0
		c.TrafficUsedTX = 0
		c.TrafficResetDate = currentMonth
		config.SaveConfig()
	}

	// Accumulate new delta since last call
	lastTrafficSnapshotMu.Lock()
	if c.Status == "running" {
		rx, tx := m.getContainerNetworkBytes(c.LxcName())
		prev, exists := lastTrafficSnapshot[c.LxcName()]
		if exists && rx >= prev.RXBytes && tx >= prev.TXBytes {
			c.TrafficUsedRX += int64(rx - prev.RXBytes)
			c.TrafficUsedTX += int64(tx - prev.TXBytes)
			config.SaveConfig()
		}
		lastTrafficSnapshot[c.LxcName()] = trafficSample{RXBytes: rx, TXBytes: tx}
	}
	lastTrafficSnapshotMu.Unlock()

	totalUsed := c.TrafficUsedRX + c.TrafficUsedTX
	limitGB := 0
	usedPct := 0.0
	if c.TrafficMode == "in_out" {
		limitGB = c.TrafficInGB + c.TrafficOutGB
		inUsed := float64(c.TrafficUsedRX)
		outUsed := float64(c.TrafficUsedTX)
		inLimit := float64(c.TrafficInGB) * 1073741824
		outLimit := float64(c.TrafficOutGB) * 1073741824
		inPct := 0.0
		outPct := 0.0
		if c.TrafficInGB > 0 {
			inPct = inUsed / inLimit * 100
		}
		if c.TrafficOutGB > 0 {
			outPct = outUsed / outLimit * 100
		}
		usedPct = inPct
		if outPct > usedPct {
			usedPct = outPct
		}
	} else {
		limitGB = c.MonthlyTrafficGB
		if limitGB > 0 {
			usedPct = float64(totalUsed) / float64(limitGB*1073741824) * 100
		}
	}

	return map[string]interface{}{
		"total_used_bytes": totalUsed,
		"rx_used_bytes":    c.TrafficUsedRX,
		"tx_used_bytes":    c.TrafficUsedTX,
		"mode":             c.TrafficMode,
		"limit_gb":         limitGB,
		"in_limit_gb":      c.TrafficInGB,
		"out_limit_gb":     c.TrafficOutGB,
		"used_pct":         usedPct,
		"reset_date":       c.TrafficResetDate,
	}
}

// ToJSON converts data to JSON bytes
func ToJSON(v interface{}) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

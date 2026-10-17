// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/internal/ocilayer"
	"github.com/agent-substrate/substrate/internal/ocilayer/adapter"
	"github.com/agent-substrate/substrate/internal/proto/snapshots"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// ProviderNydusDragonfly identifies the cross-node Nydus RAFS v6 + Dragonfly
	// v2 P2P (Kernel EROFS + fscache) remote snapshotter provider.
	ProviderNydusDragonfly = "nydus-dragonfly"

	// DefaultNydusSocket is the default UNIX socket path for containerd-nydus-grpc.
	DefaultNydusSocket = "/run/containerd-nydus/containerd-nydus-grpc.sock"

	// DefaultNydusDockerConfigPath is the default Docker config.json path shared
	// with containerd-nydus-grpc for registry authentication.
	DefaultNydusDockerConfigPath = "/run/containerd-nydus/docker/config.json"

	// EnvDragonflyProxyAddr is the environment variable configuring the local
	// Dragonfly dfdaemon HTTP proxy endpoint used by nydusd.
	EnvDragonflyProxyAddr = "DRAGONFLY_PROXY_ADDR"

	// DefaultDragonflyProxyAddr is the default local Dragonfly dfdaemon HTTP
	// proxy address on each worker node.
	DefaultDragonflyProxyAddr = "http://127.0.0.1:65001"

	// EnvDragonflySchedulerAddr is the environment variable configuring the
	// cluster-wide Dragonfly Scheduler gRPC endpoint used for cross-node P2P
	// piece scheduling.
	EnvDragonflySchedulerAddr = "DRAGONFLY_SCHEDULER_ADDR"

	// DefaultDragonflySchedulerAddr is the default Kubernetes DNS address of the
	// Dragonfly Scheduler service in ate-system.
	DefaultDragonflySchedulerAddr = "dragonfly-scheduler.ate-system.svc.cluster.local:8002"

	// EnvDragonflyManagerAddr is the environment variable configuring the
	// cluster-wide Dragonfly Manager gRPC endpoint.
	EnvDragonflyManagerAddr = "DRAGONFLY_MANAGER_ADDR"

	// DefaultDragonflyManagerAddr is the default Kubernetes DNS address of the
	// Dragonfly Manager service in ate-system.
	DefaultDragonflyManagerAddr = "dragonfly-manager.ate-system.svc.cluster.local:65003"

	// DefaultProcFilesystemsPath is the procfs path used to verify that the
	// kernel EROFS filesystem driver is registered.
	DefaultProcFilesystemsPath = "/proc/filesystems"

	// DefaultCachefilesDevPath is the character device path required by Nydus
	// fscache mode to serve EROFS chunks in-kernel.
	DefaultCachefilesDevPath = "/dev/cachefiles"

	// EROFSSuperMagic is the Linux kernel statfs f_type magic number for EROFS (0xE0F5E1E2).
	EROFSSuperMagic = 0xE0F5E1E2

	// FUSESuperMagic is the Linux kernel statfs f_type magic number for FUSE (0x65735546).
	FUSESuperMagic = 0x65735546

	// LabelNydusBootstrap marks the Nydus RAFS v6 EROFS bootstrap layer.
	LabelNydusBootstrap = "containerd.io/snapshot/nydus-bootstrap"

	// LabelNydusBlob marks a Nydus RAFS data-blob layer.
	LabelNydusBlob = "containerd.io/snapshot/nydus-blob"

	// LabelNydusFSVersion records the Nydus RAFS format version ("6" for EROFS).
	LabelNydusFSVersion = "containerd.io/snapshot/nydus-fs-version"
)

// LayerReaderFunc returns an uncompressed tar stream for a specific layer (used
// when unpacking the Nydus RAFS v6 EROFS bootstrap layer).
type LayerReaderFunc func(ctx context.Context, imageRef string, auth *ocilayer.AuthConfig, layerIndex int, layerDigest string) (io.ReadCloser, error)

// LayerAnnotationsFunc returns the per-layer OCI manifest annotations for an image.
type LayerAnnotationsFunc func(ctx context.Context, imageRef string, auth *ocilayer.AuthConfig) ([]map[string]string, error)

// MountFSFunc mounts a kernel filesystem (such as -t erofs) at targetDir.
type MountFSFunc func(source, targetDir, fstype string, flags uintptr, data string) error

// UnmountFSFunc unmounts targetDir.
type UnmountFSFunc func(targetDir string, flags int) error

// StatFSFunc returns the filesystem type magic (f_type) for path.
type StatFSFunc func(path string) (int64, error)

// NydusDragonflyConfig configures the cross-node Nydus + Dragonfly v2
// (EROFS/fscache) OCILayerService adapter.
type NydusDragonflyConfig struct {
	DockerConfigPath       string
	DragonflyProxyAddr     string
	DragonflySchedulerAddr string
	DragonflyManagerAddr   string
	VerifyDragonflyPing    bool
	HTTPClient             *http.Client
	ProcFilesystemsPath    string
	CachefilesDevPath      string
	SkipKernelCheck        bool
	LayerReader            LayerReaderFunc
	LayerAnnotations       LayerAnnotationsFunc
	MountFS                MountFSFunc
	UnmountFS              UnmountFSFunc
	StatFS                 StatFSFunc
	AdapterOpts            []adapter.Option
}

// NydusDragonflyDriver wraps the shared adapter.Driver with strict Kernel EROFS
// + fscache enforcement (no fusedev fallback), cross-node Dragonfly v2 P2P
// proxy/scheduler/manager configuration and health probing, Nydus RAFS v6
// manifest label propagation, and EROFS mount handling.
type NydusDragonflyDriver struct {
	*adapter.Driver
	dockerSyncer           *adapter.DockerConfigSyncer
	dragonflyProxyAddr     string
	dragonflySchedulerAddr string
	dragonflyManagerAddr   string
	verifyDragonflyPing    bool
	httpClient             *http.Client
	procFilesystemsPath    string
	cachefilesDevPath      string
	skipKernelCheck        bool
	layerReader            LayerReaderFunc
	layerAnnotations       LayerAnnotationsFunc
	mountFS                MountFSFunc
	unmountFS              UnmountFSFunc
	statFS                 StatFSFunc

	mu                sync.Mutex
	cachedAnnotations map[string][]map[string]string
	mountedDirs       map[string][]string
}

// ResolveDragonflyProxyAddr returns the local Dragonfly dfdaemon HTTP proxy
// endpoint from the explicit flag value, the DRAGONFLY_PROXY_ADDR environment
// variable, or DefaultDragonflyProxyAddr, validating that it is a well-formed
// HTTP(S) URL.
func ResolveDragonflyProxyAddr(flagVal string) (string, error) {
	addr := strings.TrimSpace(flagVal)
	if addr == "" {
		addr = strings.TrimSpace(os.Getenv(EnvDragonflyProxyAddr))
	}
	if addr == "" {
		addr = DefaultDragonflyProxyAddr
	}
	u, err := url.Parse(addr)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("invalid Dragonfly P2P proxy endpoint %q (expected http:// or https:// URL)", addr)
	}
	return strings.TrimRight(addr, "/"), nil
}

// ResolveDragonflyHostPort resolves a host:port endpoint from flagVal, envVar,
// or defaultVal and validates that it has both a non-empty host and port.
func ResolveDragonflyHostPort(flagVal, envVar, defaultVal, label string) (string, error) {
	addr := strings.TrimSpace(flagVal)
	if addr == "" && envVar != "" {
		addr = strings.TrimSpace(os.Getenv(envVar))
	}
	if addr == "" {
		addr = defaultVal
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return "", fmt.Errorf("invalid Dragonfly %s endpoint %q (expected host:port)", label, addr)
	}
	return addr, nil
}

// VerifyKernelEROFSSupport checks that the Linux kernel has the EROFS
// filesystem registered in /proc/filesystems and that /dev/cachefiles exists.
// It returns an explicit error if either is missing instead of falling back to
// userspace FUSE (fusedev).
func VerifyKernelEROFSSupport(procFilesystemsPath, cachefilesDevPath string) error {
	if procFilesystemsPath == "" {
		procFilesystemsPath = DefaultProcFilesystemsPath
	}
	if cachefilesDevPath == "" {
		cachefilesDevPath = DefaultCachefilesDevPath
	}
	b, err := os.ReadFile(procFilesystemsPath)
	if err != nil {
		return fmt.Errorf("kernel EROFS + fscache is required for ocilayer-nydus-dragonfly: reading %s failed (fusedev fallback is disabled): %w", procFilesystemsPath, err)
	}
	hasEROFS := false
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[len(fields)-1] == "erofs" {
			hasEROFS = true
			break
		}
	}
	if !hasEROFS {
		return fmt.Errorf("kernel EROFS + fscache is required for ocilayer-nydus-dragonfly: 'erofs' filesystem not registered in %s (GKE COS disables CONFIG_EROFS_FS; run on an EROFS-enabled kernel with modprobe erofs; fusedev fallback is disabled)", procFilesystemsPath)
	}
	if _, err := os.Stat(cachefilesDevPath); err != nil {
		return fmt.Errorf("kernel EROFS + fscache is required for ocilayer-nydus-dragonfly: %s is not available (run in a privileged container with modprobe cachefiles; fusedev fallback is disabled): %w", cachefilesDevPath, err)
	}
	return nil
}

// NewNydusDragonfly creates a cross-node Nydus + Dragonfly v2 OCILayerService
// driver with strict Kernel EROFS + fscache verification and Dragonfly dfdaemon
// health probing.
func NewNydusDragonfly(cfg NydusDragonflyConfig) (*NydusDragonflyDriver, error) {
	dfAddr, err := ResolveDragonflyProxyAddr(cfg.DragonflyProxyAddr)
	if err != nil {
		return nil, err
	}
	schedAddr, err := ResolveDragonflyHostPort(cfg.DragonflySchedulerAddr, EnvDragonflySchedulerAddr, DefaultDragonflySchedulerAddr, "scheduler")
	if err != nil {
		return nil, err
	}
	mgrAddr, err := ResolveDragonflyHostPort(cfg.DragonflyManagerAddr, EnvDragonflyManagerAddr, DefaultDragonflyManagerAddr, "manager")
	if err != nil {
		return nil, err
	}
	procPath := cfg.ProcFilesystemsPath
	if procPath == "" {
		procPath = DefaultProcFilesystemsPath
	}
	cachefilesPath := cfg.CachefilesDevPath
	if cachefilesPath == "" {
		cachefilesPath = DefaultCachefilesDevPath
	}
	if !cfg.SkipKernelCheck {
		if err := VerifyKernelEROFSSupport(procPath, cachefilesPath); err != nil {
			return nil, err
		}
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: adapter.DefaultProbeTimeout,
		}
	}

	nd := &NydusDragonflyDriver{
		dockerSyncer:           adapter.NewDockerConfigSyncer(cfg.DockerConfigPath),
		dragonflyProxyAddr:     dfAddr,
		dragonflySchedulerAddr: schedAddr,
		dragonflyManagerAddr:   mgrAddr,
		verifyDragonflyPing:    cfg.VerifyDragonflyPing,
		httpClient:             httpClient,
		procFilesystemsPath:    procPath,
		cachefilesDevPath:      cachefilesPath,
		skipKernelCheck:        cfg.SkipKernelCheck,
		layerReader:            cfg.LayerReader,
		layerAnnotations:       cfg.LayerAnnotations,
		mountFS:                cfg.MountFS,
		unmountFS:              cfg.UnmountFS,
		statFS:                 cfg.StatFS,
		cachedAnnotations:      make(map[string][]map[string]string),
		mountedDirs:            make(map[string][]string),
	}
	if nd.mountFS == nil {
		nd.mountFS = syscall.Mount
	}
	if nd.unmountFS == nil {
		nd.unmountFS = syscall.Unmount
	}
	if nd.statFS == nil {
		nd.statFS = defaultStatFS
	}

	opts := append([]adapter.Option{
		adapter.WithLayerMounter(nd.mountLayers),
		adapter.WithWarmHitHook(nd.onWarmHit),
		adapter.WithWorkDirTeardownHook(nd.unmountWorkDir),
		adapter.WithProbeHook(nd.probeDragonfly),
	}, cfg.AdapterOpts...)

	base, err := adapter.NewDriver(ProviderNydusDragonfly, DefaultNydusSocket, "nydus", opts...)
	if err != nil {
		return nil, err
	}
	nd.Driver = base
	if nd.layerReader == nil {
		nd.layerReader = nd.defaultLayerReader
	}
	if nd.layerAnnotations == nil {
		nd.layerAnnotations = nd.defaultLayerAnnotations
	}
	return nd, nil
}

// DragonflyProxyAddr returns the configured local Dragonfly dfdaemon HTTP proxy endpoint.
func (nd *NydusDragonflyDriver) DragonflyProxyAddr() string {
	return nd.dragonflyProxyAddr
}

// DragonflySchedulerAddr returns the configured cluster-wide Dragonfly Scheduler endpoint.
func (nd *NydusDragonflyDriver) DragonflySchedulerAddr() string {
	return nd.dragonflySchedulerAddr
}

// DragonflyManagerAddr returns the configured cluster-wide Dragonfly Manager endpoint.
func (nd *NydusDragonflyDriver) DragonflyManagerAddr() string {
	return nd.dragonflyManagerAddr
}

// PingDragonflyProxy probes the local dfdaemon /server/ping endpoint to ensure
// the Dragonfly P2P proxy is reachable before Nydus attempts to stream chunks.
func (nd *NydusDragonflyDriver) PingDragonflyProxy(ctx context.Context) error {
	var candidates []string
	if u, err := url.Parse(nd.dragonflyProxyAddr); err == nil && u.Port() == "65001" {
		candidates = append(candidates, fmt.Sprintf("%s://%s:40901/server/ping", u.Scheme, u.Hostname()))
	}
	candidates = append(candidates, nd.dragonflyProxyAddr+"/server/ping")
	var lastErr error
	for _, pingURL := range candidates {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, pingURL, nil)
		if err != nil {
			lastErr = fmt.Errorf("creating Dragonfly ping request %q: %w", pingURL, err)
			continue
		}
		resp, err := nd.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("probing Dragonfly dfdaemon at %q: %w", pingURL, err)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("Dragonfly dfdaemon ping %q returned HTTP %d", pingURL, resp.StatusCode)
	}
	return lastErr
}

func (nd *NydusDragonflyDriver) probeDragonfly(ctx context.Context) error {
	if !nd.verifyDragonflyPing {
		return nil
	}
	if err := nd.PingDragonflyProxy(ctx); err != nil {
		slog.WarnContext(ctx, "Dragonfly dfdaemon proxy ping failed during Probe",
			slog.String("proxyAddr", nd.dragonflyProxyAddr),
			slog.String("schedulerAddr", nd.dragonflySchedulerAddr),
			slog.Any("err", err))
		return err
	}
	return nil
}

func (nd *NydusDragonflyDriver) onWarmHit(ctx context.Context, _ string, req *ocilayer.PrepareRequest) {
	if req == nil {
		return
	}
	nd.dockerSyncer.Sync(ctx, nd.Driver.Keychain(), req.ImageRef, req.AuthConfig, false)
}

func (nd *NydusDragonflyDriver) mountLayers(ctx context.Context, env *adapter.MountEnv) (*adapter.MountedLayers, error) {
	if nd.verifyDragonflyPing {
		pingCtx, cancel := context.WithTimeout(ctx, adapter.DefaultProbeTimeout)
		err := nd.PingDragonflyProxy(pingCtx)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("dragonfly P2P proxy unavailable (scheduler=%s): %w", nd.dragonflySchedulerAddr, err)
		}
	}

	nd.dockerSyncer.Sync(ctx, nd.Driver.Keychain(), env.ImageRef, env.AuthConfig, true)

	imgKey := adapter.SanitizePathKey(env.ImageRef)
	runID := fmt.Sprintf("%x", time.Now().UnixNano())
	nsCtx := env.Driver.WithNamespaceContext(ctx)

	res := &adapter.MountedLayers{}

	for i, c := range env.ChainInfos {
		prepKey := fmt.Sprintf("%s-%s-l%d-prep", imgKey, runID, i)
		viewKey := fmt.Sprintf("%s-%s-l%d-view", imgKey, runID, i)
		var layerDigest string
		if i < len(env.LayerDigests) {
			layerDigest = env.LayerDigests[i]
		}
		labels := adapter.StandardCRILayerLabels(env.CanonicalRef, env.ImageDigest, env.LayerDigests, i, c.ChainID)
		extraLabels, err := nd.layerLabelsForIndex(ctx, env.ImageRef, env.AuthConfig, i, layerDigest, c)
		if err != nil {
			return res, fmt.Errorf("layer %d (%s) labels: %w", i, c.ChainID, err)
		}
		for k, v := range extraLabels {
			labels[k] = v
		}

		lk := env.Driver.AcquireChainLock(c.ChainID)
		lk.Lock()
		statStart := time.Now()
		committed, err := adapter.HasSnapshot(nsCtx, env.Client, env.Snapshotter, c.ChainID)
		res.StatDur += time.Since(statStart)
		if err == nil && !committed {
			prepStart := time.Now()
			prepResp, prepErr := env.Client.Prepare(nsCtx, &snapshots.PrepareSnapshotRequest{
				Snapshotter: env.Snapshotter,
				Key:         prepKey,
				Parent:      c.ParentChainID,
				Labels:      labels,
			})
			res.PrepareDur += time.Since(prepStart)
			switch status.Code(prepErr) {
			case codes.AlreadyExists:
				confirmStart := time.Now()
				found, sErr := adapter.HasSnapshot(nsCtx, env.Client, env.Snapshotter, c.ChainID)
				res.StatDur += time.Since(confirmStart)
				if sErr != nil {
					err = sErr
				} else if !found {
					err = errors.New("prepare returned AlreadyExists but the chain ID snapshot does not exist")
				}
			case codes.OK:
				if matErr := nd.materializeNydusLayer(ctx, env.Client, env.Snapshotter, env.ImageRef, env.AuthConfig, i, layerDigest, c, prepKey, labels, prepResp.GetMounts()); matErr != nil {
					adapter.RemoveSnapshot(nsCtx, env.Client, env.Snapshotter, prepKey)
					err = matErr
				} else {
					res.CommittedKeys = append(res.CommittedKeys, c.ChainID)
				}
			default:
				adapter.RemoveSnapshot(nsCtx, env.Client, env.Snapshotter, prepKey)
				err = fmt.Errorf("preparing snapshot: %w", prepErr)
			}
		}
		lk.Unlock()
		if err != nil {
			return res, fmt.Errorf("layer %d (%s): %w", i, c.ChainID, err)
		}

		// Nydus data-blob layers (0 .. N-2) are registered in the snapshotter
		// parent chain; only the final RAFS v6 EROFS bootstrap layer is viewed
		// and mounted as the unified EROFS image tree.
		if labels[LabelNydusBlob] == "true" && labels[LabelNydusBootstrap] != "true" {
			continue
		}

		viewStart := time.Now()
		mounts, err := adapter.ViewLayerMounts(nsCtx, env.Client, env.Snapshotter, viewKey, c.ChainID)
		res.ViewDur += time.Since(viewStart)
		if err != nil {
			return res, fmt.Errorf("layer %d (%s): %w", i, c.ChainID, err)
		}
		res.SnapshotKeys = append(res.SnapshotKeys, viewKey)

		mountDir, err := nd.resolveEROFSMount(ctx, env.ImageWorkDir, len(res.MountDirs), mounts)
		if err != nil {
			return res, fmt.Errorf("resolving mount for layer %d (%s): %w", i, c.ChainID, err)
		}
		res.MountDirs = append(res.MountDirs, mountDir)
	}

	return res, nil
}

func defaultStatFS(path string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return int64(st.Type), nil
}

func (nd *NydusDragonflyDriver) verifyMountIsEROFS(mountDir string) error {
	if nd.skipKernelCheck && nd.statFS == nil {
		return nil
	}
	fsType, err := nd.statFS(mountDir)
	if err != nil {
		return fmt.Errorf("statfs on Nydus mount %q failed: %w", mountDir, err)
	}
	if fsType == FUSESuperMagic {
		return fmt.Errorf("nydus mount %q has FUSE filesystem type (0x%x) instead of Kernel EROFS (0x%x); fusedev fallback is disabled", mountDir, fsType, int64(EROFSSuperMagic))
	}
	if fsType != EROFSSuperMagic {
		return fmt.Errorf("nydus mount %q has unexpected filesystem type 0x%x; Kernel EROFS (0x%x) is strictly required", mountDir, fsType, int64(EROFSSuperMagic))
	}
	return nil
}

func (nd *NydusDragonflyDriver) resolveEROFSMount(_ context.Context, imageWorkDir string, viewIndex int, mounts []*snapshots.Mount) (string, error) {
	for _, m := range mounts {
		if m == nil {
			continue
		}
		if m.Type == "erofs" || m.Type == "fuse.nydus-overlayfs" {
			if m.Type != "erofs" {
				return "", fmt.Errorf("snapshotter returned non-EROFS mount type %q (options=%v); Kernel EROFS (fscache) is strictly required and fusedev fallback is disabled", m.Type, m.Options)
			}
			targetDir := filepath.Join(imageWorkDir, fmt.Sprintf("erofs-mnt-%d", viewIndex))
			if err := os.MkdirAll(targetDir, 0o755); err != nil {
				return "", fmt.Errorf("creating EROFS mount point %s: %w", targetDir, err)
			}
			var dataOpts []string
			for _, opt := range m.Options {
				if opt == "ro" || opt == "rw" || opt == "nosuid" || opt == "nodev" {
					continue
				}
				dataOpts = append(dataOpts, opt)
			}
			source := m.Source
			if source == "" {
				source = "erofs"
			}
			if err := nd.mountFS(source, targetDir, "erofs", syscall.MS_RDONLY, strings.Join(dataOpts, ",")); err != nil {
				return "", fmt.Errorf("mounting kernel EROFS (%s, opts=%q) at %s: %w", source, strings.Join(dataOpts, ","), targetDir, err)
			}
			if err := nd.verifyMountIsEROFS(targetDir); err != nil {
				_ = nd.unmountFS(targetDir, syscall.MNT_DETACH)
				return "", err
			}
			nd.mu.Lock()
			nd.mountedDirs[imageWorkDir] = append(nd.mountedDirs[imageWorkDir], targetDir)
			nd.mu.Unlock()
			return targetDir, nil
		}
	}

	mountDir := adapter.ExtractMountDir(mounts)
	if mountDir == "" {
		return "", errors.New("no mount directory found in Nydus View response")
	}
	if err := nd.verifyMountIsEROFS(mountDir); err != nil {
		return "", err
	}
	return mountDir, nil
}

func (nd *NydusDragonflyDriver) unmountWorkDir(workDir string) {
	nd.mu.Lock()
	dirs := nd.mountedDirs[workDir]
	delete(nd.mountedDirs, workDir)
	nd.mu.Unlock()
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = nd.unmountFS(dirs[i], syscall.MNT_DETACH)
	}
}

func (nd *NydusDragonflyDriver) getLayerAnnotations(ctx context.Context, imageRef string, auth *ocilayer.AuthConfig) ([]map[string]string, error) {
	nd.mu.Lock()
	if cached, ok := nd.cachedAnnotations[imageRef]; ok {
		nd.mu.Unlock()
		return cached, nil
	}
	nd.mu.Unlock()

	layerAnns, err := nd.layerAnnotations(ctx, imageRef, auth)
	if err != nil {
		return nil, err
	}
	hasBootstrap := false
	for _, ann := range layerAnns {
		if ann[LabelNydusBootstrap] == "true" {
			if v := ann[LabelNydusFSVersion]; v != "" && v != "6" {
				return nil, fmt.Errorf("nydus image %q uses RAFS version %q, but RAFS v6 (EROFS) is required: %w", imageRef, v, ocilayer.ErrNotSupported)
			}
			hasBootstrap = true
		}
	}
	if !hasBootstrap {
		return nil, fmt.Errorf("image %q is not in Nydus RAFS v6 (EROFS) format (missing %s layer; pre-convert with 'nydusify convert --fs-version 6'): %w",
			imageRef, LabelNydusBootstrap, ocilayer.ErrNotSupported)
	}

	nd.mu.Lock()
	nd.cachedAnnotations[imageRef] = layerAnns
	nd.mu.Unlock()
	return layerAnns, nil
}

func (nd *NydusDragonflyDriver) layerLabelsForIndex(ctx context.Context, imageRef string, auth *ocilayer.AuthConfig, layerIndex int, layerDigest string, _ adapter.ChainInfo) (map[string]string, error) {
	layerAnns, err := nd.getLayerAnnotations(ctx, imageRef, auth)
	if err != nil {
		return nil, err
	}
	if layerIndex < 0 || layerIndex >= len(layerAnns) {
		return nil, fmt.Errorf("layer index %d out of range (len=%d)", layerIndex, len(layerAnns))
	}
	out := make(map[string]string, len(layerAnns[layerIndex])+3)
	for k, v := range layerAnns[layerIndex] {
		out[k] = v
	}
	out["containerd.io/snapshot/cri.image-ref"] = imageRef
	out["containerd.io/snapshot/cri.layer-digest"] = layerDigest
	if ref, err := name.ParseReference(imageRef); err == nil {
		reg := ref.Context().RegistryStr()
		repo := ref.Context().RepositoryStr()
		if reg != "" && repo != "" {
			out["containerd.io/distribution.source."+reg] = repo
		}
	}
	return out, nil
}

func (nd *NydusDragonflyDriver) materializeNydusLayer(ctx context.Context, client snapshots.SnapshotsClient, snapshotter, imageRef string, auth *ocilayer.AuthConfig, layerIndex int, layerDigest string, c adapter.ChainInfo, prepKey string, labels map[string]string, mounts []*snapshots.Mount) error {
	if labels[LabelNydusBootstrap] != "true" {
		if labels[LabelNydusBlob] == "true" {
			labels["containerd.io/snapshot.ref"] = c.ChainID
			_, err := client.Commit(nd.Driver.WithNamespaceContext(ctx), &snapshots.CommitSnapshotRequest{
				Snapshotter: snapshotter,
				Name:        c.ChainID,
				Key:         prepKey,
				Labels:      labels,
			})
			if err != nil && status.Code(err) != codes.AlreadyExists {
				return fmt.Errorf("committing nydus blob layer %d (%s): %w", layerIndex, c.ChainID, err)
			}
			return nil
		}
		return fmt.Errorf("nydus snapshotter declined non-bootstrap layer %d (%s): %w", layerIndex, layerDigest, ocilayer.ErrNotSupported)
	}

	targetDir := adapter.ExtractActiveMountDir(mounts)
	if targetDir == "" {
		return fmt.Errorf("nydus bootstrap layer %d (%s): no writable mount directory in Prepare response", layerIndex, c.ChainID)
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("creating nydus bootstrap mount directory %s: %w", targetDir, err)
	}

	rc, err := nd.layerReader(ctx, imageRef, auth, layerIndex, layerDigest)
	if err != nil {
		return fmt.Errorf("fetching nydus EROFS bootstrap layer %d (%s): %w", layerIndex, layerDigest, err)
	}
	defer func() { _ = rc.Close() }()

	if err := adapter.UnpackLayerTarToDir(ctx, rc, targetDir); err != nil {
		return fmt.Errorf("unpacking nydus EROFS bootstrap layer %d (%s) into %s: %w", layerIndex, layerDigest, targetDir, err)
	}

	labels["containerd.io/snapshot.ref"] = c.ChainID
	_, err = client.Commit(nd.Driver.WithNamespaceContext(ctx), &snapshots.CommitSnapshotRequest{
		Snapshotter: snapshotter,
		Name:        c.ChainID,
		Key:         prepKey,
		Labels:      labels,
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("committing nydus EROFS bootstrap layer %d (%s): %w", layerIndex, c.ChainID, err)
	}

	slog.InfoContext(ctx, "Materialized and committed Nydus RAFS v6 EROFS bootstrap layer",
		slog.Int("layerIndex", layerIndex),
		slog.String("layerDigest", layerDigest),
		slog.String("chainID", c.ChainID),
		slog.String("targetDir", targetDir),
		slog.String("dragonflyProxy", nd.dragonflyProxyAddr),
		slog.String("dragonflyScheduler", nd.dragonflySchedulerAddr),
		slog.String("dragonflyManager", nd.dragonflyManagerAddr))
	return nil
}

func (nd *NydusDragonflyDriver) defaultLayerAnnotations(ctx context.Context, imageRef string, authConfig *ocilayer.AuthConfig) ([]map[string]string, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return nil, fmt.Errorf("parsing image reference %q: %w", imageRef, err)
	}
	img, err := remote.Image(ref, adapter.RemoteOptions(ctx, nd.Driver.Keychain(), authConfig)...)
	if err != nil {
		return nil, fmt.Errorf("remote.Image(%q): %w", imageRef, err)
	}
	manifest, err := img.Manifest()
	if err != nil {
		return nil, fmt.Errorf("reading manifest for %q: %w", imageRef, err)
	}
	res := make([]map[string]string, len(manifest.Layers))
	for i, l := range manifest.Layers {
		ann := make(map[string]string, len(l.Annotations)+1)
		for k, v := range l.Annotations {
			ann[k] = v
		}
		if string(l.MediaType) == "application/vnd.oci.image.layer.nydus.blob.v1" {
			ann[LabelNydusBlob] = "true"
		}
		res[i] = ann
	}
	return res, nil
}

func (nd *NydusDragonflyDriver) defaultLayerReader(ctx context.Context, imageRef string, authConfig *ocilayer.AuthConfig, layerIndex int, _ string) (io.ReadCloser, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return nil, fmt.Errorf("parsing image reference %q: %w", imageRef, err)
	}
	img, err := remote.Image(ref, adapter.RemoteOptions(ctx, nd.Driver.Keychain(), authConfig)...)
	if err != nil {
		return nil, fmt.Errorf("remote.Image(%q): %w", imageRef, err)
	}
	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("listing layers for %q: %w", imageRef, err)
	}
	if layerIndex < 0 || layerIndex >= len(layers) {
		return nil, fmt.Errorf("layer index %d out of range (len=%d)", layerIndex, len(layers))
	}
	return layers[layerIndex].Uncompressed()
}

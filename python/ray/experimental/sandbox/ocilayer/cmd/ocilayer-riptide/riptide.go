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
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ocilayer"
	"github.com/agent-substrate/substrate/internal/ocilayer/adapter"
	"github.com/agent-substrate/substrate/internal/proto/riptidekeychain"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// ProviderRiptide identifies the Google Riptide v2 remote snapshotter provider.
	ProviderRiptide = "riptide"

	// DefaultRiptideSocket is the default UNIX socket path for Substrate's
	// dedicated containerd-gcfs-grpc instance.
	DefaultRiptideSocket = "/run/ate-image-streaming/gcfs-snapshotter.sock"

	// DefaultRiptideRootDir is the default isolated root directory for
	// Substrate's dedicated containerd-gcfs-grpc and gcfsd-v2 state.
	DefaultRiptideRootDir = "/var/lib/substrate/gcfs"

	// DefaultGCFSDMountPoint is the FUSE mount point for gcfsd-v2.
	DefaultGCFSDMountPoint = "/run/gcfsd/mnt"

	// DefaultGCFSDControlSocket is the default UNIX socket path for gcfsd-v2's
	// control server (--control_server_socket_path / --gcfs-socket-path).
	DefaultGCFSDControlSocket = "/run/gcfsd/grpc.sock"

	// DefaultRiptideKeychainSocket is the default UNIX socket path for Riptide's
	// isolated gcfsd keychain service (cloud.containers.riptide.fuse.keychain.Keychain).
	DefaultRiptideKeychainSocket = "/run/ate-image-streaming/gcfsd/keychain.sock"

	// DefaultKubeHome is the GKE node directory where install-riptide preloads
	// gcfsd, gcfsd-v2, and containerd-gcfs-grpc.
	DefaultKubeHome = "/home/kubernetes"

	// DefaultGCFSSnapshotterConfigPath is the config.toml path used by
	// containerd-gcfs-grpc on GKE nodes.
	DefaultGCFSSnapshotterConfigPath = "/etc/containerd-gcfs-grpc/config.toml"

	// riptideCredsPushCacheTTL bounds how long identical credentials for the
	// same canonical image reference skip redundant UpdateCreds RPCs on warm
	// lease hits.
	riptideCredsPushCacheTTL = 5 * time.Minute
)

// RiptideDaemonConfig holds the GKE node parameters used to construct the
// gcfsd / gcfsd-v2 and containerd-gcfs-grpc command lines, matching
// gke-setup-gcfs in gke-internal-configure-helper.sh as well as Substrate's
// isolated Riptide stack.
type RiptideDaemonConfig struct {
	KubeHome                     string
	RootDir                      string
	MountPoint                   string
	SnapshotterSocket            string
	GCFSSocketPath               string
	SnapshotterConfigPath        string
	ControlServerSocketPath      string
	DownloaderUDSPath            string
	SentinelFilePath             string
	DisableContainerdEvents      bool
	DisableKubeletEventReporting bool
	Port                         int
	EnableV2API                  bool
	EnableUserNamespaceSupport   bool
	EnableLayerCaching           bool
	CacheSizeMiB                 int
	KubeletVersion               string
	SecondaryBootDisks           string
	SecondaryBootDataDisks       string
	EnableRiptideImagePreloading bool
	GCFSDADCConfigPath           string
	SnapshotterADCConfigPath     string
}

func (c RiptideDaemonConfig) withDefaults() RiptideDaemonConfig {
	if c.KubeHome == "" {
		c.KubeHome = DefaultKubeHome
	}
	if c.RootDir == "" {
		c.RootDir = DefaultRiptideRootDir
	}
	if c.MountPoint == "" {
		c.MountPoint = DefaultGCFSDMountPoint
	}
	if c.SnapshotterSocket == "" {
		c.SnapshotterSocket = DefaultRiptideSocket
	}
	if c.GCFSSocketPath == "" {
		c.GCFSSocketPath = DefaultGCFSDControlSocket
	}
	if c.SnapshotterConfigPath == "" {
		c.SnapshotterConfigPath = DefaultGCFSSnapshotterConfigPath
	}
	return c
}

// BuildGCFSDCommand constructs the binary path, CLI flags, and environment
// variables for the Riptide FUSE daemon (gcfsd or gcfsd-v2), matching
// gke-setup-gcfs in gke-internal-configure-helper.sh.
func BuildGCFSDCommand(cfg RiptideDaemonConfig) (bin string, args []string, env []string) {
	cfg = cfg.withDefaults()
	gcfsdBin := "gcfsd"
	if cfg.EnableV2API {
		gcfsdBin = "gcfsd-v2"
	}
	bin = filepath.Join(cfg.KubeHome, "bin", gcfsdBin)

	layerCacheDir := filepath.Join(cfg.RootDir, "snapshotter", "layers")
	imagesInUseDBPath := filepath.Join(cfg.RootDir, "gcfsd", "images_in_use.db")

	args = append(args, "--mount_point="+cfg.MountPoint)
	if cfg.ControlServerSocketPath != "" {
		args = append(args, "--control_server_socket_path="+cfg.ControlServerSocketPath)
	}
	if cfg.DownloaderUDSPath != "" {
		args = append(args, "--downloader_uds_path="+cfg.DownloaderUDSPath)
	}
	if cfg.SentinelFilePath != "" {
		args = append(args, "--sentinel_file_path="+cfg.SentinelFilePath)
	}
	if cfg.DisableContainerdEvents {
		args = append(args, "--containerd_uds_path=", "--event_reporter_type=none")
	}
	if cfg.Port > 0 {
		args = append(args, fmt.Sprintf("--port=%d", cfg.Port))
	}
	if cfg.CacheSizeMiB > 0 {
		eachCacheSize := cfg.CacheSizeMiB / 2
		args = append(args,
			fmt.Sprintf("--max_content_cache_size_mb=%d", eachCacheSize),
			fmt.Sprintf("--max_large_files_cache_size_mb=%d", eachCacheSize),
		)
	}
	if cfg.EnableLayerCaching {
		args = append(args, "--layer_cache_dir="+layerCacheDir)
	}
	args = append(args, "--images_in_use_db_path="+imagesInUseDBPath)
	if cfg.EnableV2API && cfg.EnableUserNamespaceSupport {
		args = append(args, "--enable_id_mapping")
	}
	args = append(args,
		"--enable_pull_secret_keychain",
		"--client_name=GKE",
	)
	if cfg.KubeletVersion != "" {
		args = append(args, fmt.Sprintf("--client_version=%s", cfg.KubeletVersion))
	}
	args = append(args,
		"--enable_single_flighting=true",
		"--read_ahead_max_blocks=10",
		"--read_ahead_cache_size_ratio=0.1",
	)

	env = append(env, "GOGC=10")
	if cfg.GCFSDADCConfigPath != "" {
		env = append(env, "GCFSD_METRICS_ADC_CONFIG_PATH="+cfg.GCFSDADCConfigPath)
	}
	return bin, args, env
}

// BuildGCFSSnapshotterCommand constructs the binary path, CLI flags, and
// environment variables for containerd-gcfs-grpc, matching gke-setup-gcfs in
// gke-internal-configure-helper.sh with Substrate's isolated root and socket.
func BuildGCFSSnapshotterCommand(cfg RiptideDaemonConfig) (bin string, args []string, env []string) {
	cfg = cfg.withDefaults()
	bin = filepath.Join(cfg.KubeHome, "bin", "containerd-gcfs-grpc")

	args = append(args,
		"--log-level=info",
		"--config="+cfg.SnapshotterConfigPath,
		"--address="+cfg.SnapshotterSocket,
		"--root="+cfg.RootDir,
		"--gcfs-socket-path="+cfg.GCFSSocketPath,
	)
	if cfg.DisableKubeletEventReporting {
		args = append(args, "--enable-kubernetes-event-reporting=false")
	} else {
		args = append(args, "--enable-image-proxy-keychain-client")
	}
	if cfg.SecondaryBootDisks != "" {
		args = append(args, "--secondary-disk-mount-points="+cfg.SecondaryBootDisks)
	}
	if cfg.SecondaryBootDataDisks != "" {
		args = append(args, "--secondary-data-disk-mount-points="+cfg.SecondaryBootDataDisks)
	}
	args = append(args, "--disable-duplicate-layer-support=false")
	if cfg.EnableRiptideImagePreloading {
		args = append(args, "--enable-metric-exporter=false")
	}
	if cfg.EnableV2API {
		args = append(args, "--enable-v2")
	}

	env = append(env, "HOME=/root")
	if cfg.SnapshotterADCConfigPath != "" {
		env = append(env, "GCFS_SNAPSHOTTER_METRICS_ADC_CONFIG_PATH="+cfg.SnapshotterADCConfigPath)
	}
	return bin, args, env
}

type pushedCredEntry struct {
	digest   [32]byte
	pushedAt time.Time
}

// RiptideConfig configures the Google Riptide v2 OCILayerService adapter.
type RiptideConfig struct {
	KeychainSocket string
	KeychainClient riptidekeychain.KeychainClient
	AdapterOpts    []adapter.Option
}

// RiptideDriver wraps the shared adapter.Driver with Google Riptide v2's gcfsd
// Keychain.UpdateCreds credential management.
type RiptideDriver struct {
	*adapter.Driver
	keychainSocket string

	mu                     sync.Mutex
	keychainConn           *grpc.ClientConn
	keychainClient         riptidekeychain.KeychainClient
	keychainClientInjected bool
	pushedCreds            map[string]pushedCredEntry
}

// NewRiptide creates a Google Riptide v2 OCILayerService driver.
func NewRiptide(cfg RiptideConfig) (*RiptideDriver, error) {
	kcSocket := cfg.KeychainSocket
	if kcSocket == "" && cfg.KeychainClient == nil {
		kcSocket = DefaultRiptideKeychainSocket
	}
	rd := &RiptideDriver{
		keychainSocket:         kcSocket,
		keychainClient:         cfg.KeychainClient,
		keychainClientInjected: cfg.KeychainClient != nil,
		pushedCreds:            make(map[string]pushedCredEntry),
	}

	opts := append([]adapter.Option{
		adapter.WithLayerMounter(rd.mountLayers),
		adapter.WithWarmHitHook(rd.onWarmHit),
	}, cfg.AdapterOpts...)

	base, err := adapter.NewDriver(ProviderRiptide, DefaultRiptideSocket, "gcfs", opts...)
	if err != nil {
		return nil, err
	}
	rd.Driver = base
	return rd, nil
}

func (rd *RiptideDriver) mountLayers(ctx context.Context, env *adapter.MountEnv) (*adapter.MountedLayers, error) {
	rd.pushKeychainCreds(ctx, env.CanonicalRef, env.ImageRef, env.AuthConfig, true)
	return adapter.StandardMountLayers(ctx, env)
}

func (rd *RiptideDriver) onWarmHit(ctx context.Context, canonicalRef string, req *ocilayer.PrepareRequest) {
	if req == nil {
		return
	}
	rd.pushKeychainCreds(ctx, canonicalRef, req.ImageRef, req.AuthConfig, false)
}

// Close closes the underlying snapshotter driver and the Riptide keychain connection.
func (rd *RiptideDriver) Close() error {
	rd.mu.Lock()
	if rd.keychainConn != nil {
		_ = rd.keychainConn.Close()
		rd.keychainConn = nil
	}
	rd.mu.Unlock()
	return rd.Driver.Close()
}

func (rd *RiptideDriver) getKeychainClient() (riptidekeychain.KeychainClient, error) {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	if rd.keychainClient != nil {
		return rd.keychainClient, nil
	}
	if rd.keychainSocket == "" {
		return nil, nil
	}
	conn, err := grpc.NewClient("unix://"+rd.keychainSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	rd.keychainConn = conn
	rd.keychainClient = riptidekeychain.NewKeychainClient(conn)
	return rd.keychainClient, nil
}

func (rd *RiptideDriver) resolveAuthConfig(refStr string, reqAuth *ocilayer.AuthConfig) *riptidekeychain.AuthConfig {
	if reqAuth != nil && (reqAuth.Username != "" || reqAuth.Password != "" || reqAuth.Auth != "" || reqAuth.IdentityToken != "" || reqAuth.RegistryToken != "") {
		serverAddr := reqAuth.ServerAddress
		if serverAddr == "" {
			if ref, err := name.ParseReference(refStr); err == nil {
				serverAddr = ref.Context().RegistryStr()
			}
		}
		return &riptidekeychain.AuthConfig{
			Username:      reqAuth.Username,
			Password:      reqAuth.Password,
			Auth:          reqAuth.Auth,
			ServerAddress: serverAddr,
			IdentityToken: reqAuth.IdentityToken,
			RegistryToken: reqAuth.RegistryToken,
		}
	}
	if rd == nil || rd.Driver == nil || rd.Driver.Keychain() == nil {
		return nil
	}
	ref, err := name.ParseReference(refStr)
	if err != nil {
		return nil
	}
	authenticator, err := rd.Driver.Keychain().Resolve(ref.Context())
	if err != nil || authenticator == nil || authenticator == authn.Anonymous {
		return nil
	}
	cfg, err := authenticator.Authorization()
	if err != nil || cfg == nil {
		return nil
	}
	if cfg.Username == "" && cfg.Password == "" && cfg.Auth == "" && cfg.IdentityToken == "" && cfg.RegistryToken == "" {
		return nil
	}
	return &riptidekeychain.AuthConfig{
		Username:      cfg.Username,
		Password:      cfg.Password,
		Auth:          cfg.Auth,
		ServerAddress: ref.Context().RegistryStr(),
		IdentityToken: cfg.IdentityToken,
		RegistryToken: cfg.RegistryToken,
	}
}

func hashRiptideAuthConfig(auth *riptidekeychain.AuthConfig) [32]byte {
	return sha256.Sum256([]byte(strings.Join([]string{
		auth.GetUsername(),
		auth.GetPassword(),
		auth.GetAuth(),
		auth.GetServerAddress(),
		auth.GetIdentityToken(),
		auth.GetRegistryToken(),
	}, "\x00")))
}

func (rd *RiptideDriver) pushKeychainCreds(ctx context.Context, criImageRef, imageRef string, authConfig *ocilayer.AuthConfig, force bool) {
	if rd == nil || imageRef == "" || criImageRef == "" {
		return
	}
	if rd.keychainSocket == "" && !rd.keychainClientInjected {
		return
	}
	auth := rd.resolveAuthConfig(imageRef, authConfig)
	if auth == nil {
		return
	}
	authDigest := hashRiptideAuthConfig(auth)
	now := time.Now()
	if !force {
		rd.mu.Lock()
		prev, ok := rd.pushedCreds[criImageRef]
		rd.mu.Unlock()
		if ok && prev.digest == authDigest && now.Sub(prev.pushedAt) < riptideCredsPushCacheTTL {
			return
		}
	}
	if !rd.keychainClientInjected && rd.keychainSocket != "" {
		if _, err := os.Stat(rd.keychainSocket); err != nil {
			slog.DebugContext(ctx, "Riptide gcfsd keychain socket not present; skipping UpdateCreds",
				slog.String("socket", rd.keychainSocket),
				slog.String("image", criImageRef))
			return
		}
	}
	kcClient, err := rd.getKeychainClient()
	if err != nil || kcClient == nil {
		slog.WarnContext(ctx, "Failed to initialize Riptide gcfsd keychain client",
			slog.String("socket", rd.keychainSocket),
			slog.Any("error", err))
		return
	}
	rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := kcClient.UpdateCreds(rpcCtx, &riptidekeychain.UpdateCredsRequest{
		Image: criImageRef,
		Auth:  auth,
	}); err != nil {
		slog.WarnContext(ctx, "Failed to push credentials to Riptide gcfsd keychain",
			slog.String("socket", rd.keychainSocket),
			slog.String("image", criImageRef),
			slog.Any("error", err))
		return
	}
	rd.mu.Lock()
	if rd.pushedCreds == nil {
		rd.pushedCreds = make(map[string]pushedCredEntry)
	}
	rd.pushedCreds[criImageRef] = pushedCredEntry{
		digest:   authDigest,
		pushedAt: now,
	}
	rd.mu.Unlock()
	slog.InfoContext(ctx, "Pushed credentials to Riptide gcfsd keychain",
		slog.String("socket", rd.keychainSocket),
		slog.String("image", criImageRef),
		slog.String("server", auth.GetServerAddress()),
		slog.String("username", auth.GetUsername()))
}

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

package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/ocilayer"
	"github.com/agent-substrate/substrate/internal/proto/snapshots"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/google"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// DefaultNamespace is the default containerd namespace passed to the snapshotter.
	DefaultNamespace = "default"

	// ContainerdNamespaceHeader is the gRPC metadata key for containerd namespace.
	ContainerdNamespaceHeader = "containerd-namespace"

	// DefaultListableTimeout is the default maximum duration to wait for a layer view to become listable.
	DefaultListableTimeout = 10 * time.Second

	// DefaultListableInterval is the poll interval between readdir probe attempts.
	DefaultListableInterval = 50 * time.Millisecond

	// DefaultProbeTimeout bounds the gRPC liveness check in CanProvide so a hung
	// snapshotter daemon falls back quickly instead of stalling actor startup.
	DefaultProbeTimeout = 500 * time.Millisecond

	// DefaultDeclineTTL is how long the driver remembers that the snapshotter
	// declined an image (ErrNotSupported) before trying again.
	DefaultDeclineTTL = 10 * time.Minute

	// DefaultLeaseIdleTTL is how long the driver keeps an image's layer views
	// mounted after its reference count drops to zero.
	DefaultLeaseIdleTTL = 10 * time.Minute
)

// MountEnv provides the resolved image metadata, snapshotter client, and
// working directory to a provider's LayerMounterFunc during a cold
// PrepareImage call.
type MountEnv struct {
	Driver       *Driver
	Client       snapshots.SnapshotsClient
	Snapshotter  string
	ImageRef     string
	CanonicalRef string
	ImageDigest  string
	ImageWorkDir string
	AuthConfig   *ocilayer.AuthConfig
	ChainInfos   []ChainInfo
	LayerDigests []string
}

// MountedLayers holds the read-only host directories and snapshotter keys
// produced by a provider's LayerMounterFunc.
type MountedLayers struct {
	// MountDirs are the read-only host directories (ordered bottom-to-top) to be
	// verified for listability and symlinked as layer-0/fs .. layer-(K-1)/fs.
	MountDirs []string

	// SnapshotKeys are the snapshotter view keys to remove when the image lease
	// is evicted or rolled back.
	SnapshotKeys []string

	// CommittedKeys are any locally committed snapshot keys to remove if cold
	// preparation fails midway and rolls back.
	CommittedKeys []string

	// Timing breakdown durations reported by the provider.
	StatDur    time.Duration
	PrepareDur time.Duration
	ViewDur    time.Duration
}

// LayerMounterFunc prepares and mounts the layers for an image in the
// underlying snapshotter and returns the read-only host mount directories to
// expose as layer-0/fs .. layer-(K-1)/fs. When not overridden via
// WithLayerMounter, Driver defaults to StandardMountLayers.
type LayerMounterFunc func(ctx context.Context, env *MountEnv) (*MountedLayers, error)

// WarmHitHookFunc is an optional hook invoked when an existing warm lease is
// reused (for example, to refresh short-lived credentials on a warm hit).
type WarmHitHookFunc func(ctx context.Context, canonicalRef string, req *ocilayer.PrepareRequest)

// WorkDirTeardownHookFunc is an optional cleanup hook invoked before an image
// lease's working directory is removed (for example, to unmount kernel mounts
// inside workDir).
type WorkDirTeardownHookFunc func(workDir string)

// Option configures a Driver.
type Option func(*Driver)

// WithName sets the provider name reported by Name().
func WithName(name string) Option {
	return func(d *Driver) {
		d.name = name
	}
}

// WithSocketPath sets the snapshotter daemon UNIX socket path.
func WithSocketPath(p string) Option {
	return func(d *Driver) {
		d.socket = p
	}
}

// WithWorkDir sets the base directory for layer wrapper directories.
func WithWorkDir(dir string) Option {
	return func(d *Driver) {
		d.workDir = dir
	}
}

// WithNamespace sets the containerd namespace passed in gRPC requests.
func WithNamespace(ns string) Option {
	return func(d *Driver) {
		d.namespace = ns
	}
}

// WithSnapshotterName sets the snapshotter plugin name passed in Prepare/View requests.
func WithSnapshotterName(name string) Option {
	return func(d *Driver) {
		d.snapshotterName = name
	}
}

// WithListableTimeout sets the maximum duration to wait for a mount to become listable.
func WithListableTimeout(timeout time.Duration) Option {
	return func(d *Driver) {
		d.listableTimeout = timeout
	}
}

// WithListableInterval sets the polling interval between listability probe attempts.
func WithListableInterval(interval time.Duration) Option {
	return func(d *Driver) {
		d.listableInterval = interval
	}
}

// WithDeclineTTL sets how long declined images are remembered in memory.
func WithDeclineTTL(ttl time.Duration) Option {
	return func(d *Driver) {
		d.declineTTL = ttl
	}
}

// WithLeaseIdleTTL sets how long an idle image lease (refCount == 0) is kept
// mounted before its snapshotter views and wrapper directory are removed.
func WithLeaseIdleTTL(ttl time.Duration) Option {
	return func(d *Driver) {
		d.leaseIdleTTL = ttl
	}
}

// WithSnapshotsClient injects a SnapshotsClient (used in unit tests).
func WithSnapshotsClient(c snapshots.SnapshotsClient) Option {
	return func(d *Driver) {
		d.snapshotsClient = c
		d.clientInjected = true
	}
}

// ImageResolverFunc resolves an image reference to its manifest digest, config, diffIDs, and layer digests.
type ImageResolverFunc func(ctx context.Context, ref string, auth *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error)

// WithImageResolver sets the resolver function used to obtain image diffIDs and config.
func WithImageResolver(fn ImageResolverFunc) Option {
	return func(d *Driver) {
		d.imageResolver = fn
	}
}

// WithKeychain sets the credential keychain used for resolving image metadata.
func WithKeychain(k authn.Keychain) Option {
	return func(d *Driver) {
		d.keychain = k
	}
}

// WithLayerMounter sets the provider-specific layer mounter executed during cold
// PrepareImage calls. When unset, Driver uses StandardMountLayers.
func WithLayerMounter(fn LayerMounterFunc) Option {
	return func(d *Driver) {
		d.layerMounter = fn
	}
}

// WithWarmHitHook sets an optional hook invoked when PrepareImage reuses a
// warm lease.
func WithWarmHitHook(fn WarmHitHookFunc) Option {
	return func(d *Driver) {
		d.warmHitHook = fn
	}
}

// WithWorkDirTeardownHook sets an optional cleanup hook invoked before an image
// lease's working directory is removed.
func WithWorkDirTeardownHook(fn WorkDirTeardownHookFunc) Option {
	return func(d *Driver) {
		d.workDirTeardownHook = fn
	}
}

// ProbeHookFunc is an optional liveness check invoked by Probe (for example, to
// verify that a local P2P proxy daemon is healthy in addition to the
// snapshotter gRPC socket).
type ProbeHookFunc func(ctx context.Context) error

// WithProbeHook sets an optional additional health check invoked during Probe.
func WithProbeHook(fn ProbeHookFunc) Option {
	return func(d *Driver) {
		d.probeHook = fn
	}
}

type imageLease struct {
	imageRef      string
	digest        string
	config        *v1.Config
	snapshotKeys  []string
	committedKeys []string
	layers        []string
	workDir       string
	refCount      int
	idleTimer     *time.Timer
	evicted       bool
}

type resolvedImageMeta struct {
	digest       string
	config       *v1.Config
	diffIDs      []string
	layerDigests []string
}

type inflightPrep struct {
	done chan struct{}
	err  error
}

// Driver manages the shared OCILayerService lease lifecycle: singleflight
// coalescing, reference counts, idle TTL retention, decline negative caching,
// wrapper directory creation (<workDir>/<imgKey>/layer-<i>/fs), and crash
// reconciliation. Provider-specific snapshotter preparation is delegated to
// LayerMounterFunc.
type Driver struct {
	name                string
	socket              string
	workDir             string
	namespace           string
	snapshotterName     string
	listableTimeout     time.Duration
	listableInterval    time.Duration
	declineTTL          time.Duration
	leaseIdleTTL        time.Duration
	imageResolver       ImageResolverFunc
	keychain            authn.Keychain
	layerMounter        LayerMounterFunc
	warmHitHook         WarmHitHookFunc
	workDirTeardownHook WorkDirTeardownHookFunc
	probeHook           ProbeHookFunc

	mu              sync.Mutex
	closed          bool
	conn            *grpc.ClientConn
	snapshotsClient snapshots.SnapshotsClient
	clientInjected  bool
	leases          map[string]*imageLease
	resolvedMeta    map[string]*resolvedImageMeta
	declined        map[string]time.Time
	inflight        map[string]*inflightPrep
	chainLocks      map[string]*sync.Mutex
}

// NewDriver creates a new shared Driver for a remote snapshotter provider.
func NewDriver(name, defaultSocket, snapshotterName string, opts ...Option) (*Driver, error) {
	d := &Driver{
		name:             name,
		socket:           defaultSocket,
		workDir:          filepath.Join(ateompath.ImageStreamingDir, name),
		namespace:        DefaultNamespace,
		snapshotterName:  snapshotterName,
		listableTimeout:  DefaultListableTimeout,
		listableInterval: DefaultListableInterval,
		declineTTL:       DefaultDeclineTTL,
		leaseIdleTTL:     DefaultLeaseIdleTTL,
		leases:           make(map[string]*imageLease),
		resolvedMeta:     make(map[string]*resolvedImageMeta),
		declined:         make(map[string]time.Time),
		inflight:         make(map[string]*inflightPrep),
		chainLocks:       make(map[string]*sync.Mutex),
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.imageResolver == nil {
		d.imageResolver = d.defaultImageResolver
	}
	if d.layerMounter == nil {
		d.layerMounter = StandardMountLayers
	}
	return d, nil
}

// Keychain returns the configured authn.Keychain, or the default
// multi-keychain (google.Keychain + authn.DefaultKeychain) when none was
// explicitly set via WithKeychain.
func (d *Driver) Keychain() authn.Keychain {
	if d.keychain != nil {
		return d.keychain
	}
	return authn.NewMultiKeychain(google.Keychain, authn.DefaultKeychain)
}

// WithNamespaceContext attaches the containerd-namespace metadata header to ctx.
func (d *Driver) WithNamespaceContext(ctx context.Context) context.Context {
	ns := d.namespace
	if ns == "" {
		ns = DefaultNamespace
	}
	return metadata.AppendToOutgoingContext(ctx, ContainerdNamespaceHeader, ns)
}

// AcquireChainLock returns a per-ChainID mutex so concurrent PrepareImage
// calls for different images sharing base layers serialize Stat+Prepare per
// ChainID.
func (d *Driver) AcquireChainLock(chainID string) *sync.Mutex {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.chainLocks == nil {
		d.chainLocks = make(map[string]*sync.Mutex)
	}
	lk, ok := d.chainLocks[chainID]
	if !ok {
		lk = &sync.Mutex{}
		d.chainLocks[chainID] = lk
	}
	return lk
}

// Name returns the provider identifier.
func (d *Driver) Name() string {
	return d.name
}

func resolveSocketPath(p string) string {
	if p == "" {
		return p
	}
	fi, err := os.Stat(p)
	if err != nil {
		if !strings.HasSuffix(p, ".sock") {
			if _, err2 := os.Stat(p + ".sock"); err2 == nil {
				return p + ".sock"
			}
			candidate := filepath.Join(p, "containerd-gcfs-grpc.sock")
			if _, err2 := os.Stat(candidate); err2 == nil {
				return candidate
			}
		}
		return p
	}
	if fi.IsDir() {
		candidate := filepath.Join(p, "containerd-gcfs-grpc.sock")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		candidate = filepath.Join(p, filepath.Base(p)+".sock")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		if entries, err := os.ReadDir(p); err == nil {
			for _, e := range entries {
				if strings.HasSuffix(e.Name(), ".sock") {
					return filepath.Join(p, e.Name())
				}
			}
		}
	}
	return p
}

// SocketPath returns the resolved snapshotter UNIX socket path.
func (d *Driver) SocketPath() string {
	return resolveSocketPath(d.socket)
}

func (d *Driver) isDeclinedLocked(ref string, now time.Time) bool {
	if ref == "" {
		return false
	}
	exp, ok := d.declined[ref]
	if !ok {
		return false
	}
	if !now.Before(exp) {
		delete(d.declined, ref)
		return false
	}
	return true
}

// CanProvide checks whether the image was recently declined and whether the
// remote snapshotter daemon answers a bounded Probe RPC. If the image already
// has a warm active or idle lease whose layers exist on disk, CanProvide
// returns true immediately without a Probe RPC.
func (d *Driver) CanProvide(ctx context.Context, req *ocilayer.PrepareRequest) (bool, error) {
	d.mu.Lock()
	if req != nil && req.ImageRef != "" {
		if lease, ok := d.leases[req.ImageRef]; ok && !lease.evicted && lease.refCount >= 0 && len(lease.layers) > 0 {
			layers := append([]string(nil), lease.layers...)
			d.mu.Unlock()
			if layersExist(layers) {
				return true, nil
			}
			d.mu.Lock()
		}
		if d.isDeclinedLocked(req.ImageRef, time.Now()) {
			d.mu.Unlock()
			return false, nil
		}
	}
	injected := d.clientInjected
	d.mu.Unlock()

	sock := d.SocketPath()
	if !injected {
		if _, err := os.Stat(sock); err != nil {
			return false, nil
		}
	}
	probeCtx, cancel := context.WithTimeout(ctx, DefaultProbeTimeout)
	defer cancel()
	if err := d.Probe(probeCtx); err != nil {
		slog.Debug("remote snapshotter socket present but unreachable", "provider", d.name, "socket", sock, "error", err)
		return false, nil
	}
	return true, nil
}

const probeKey = "ate-streaming-probe"

// Probe reports whether the snapshotter answers Snapshots.v1 requests.
func (d *Driver) Probe(ctx context.Context) error {
	d.mu.Lock()
	client := d.snapshotsClient
	d.mu.Unlock()
	if client == nil {
		conn, err := grpc.NewClient("unix://"+d.SocketPath(),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("creating client: %w", err)
		}
		defer func() { _ = conn.Close() }()
		client = snapshots.NewSnapshotsClient(conn)
	}
	snapshotter := d.snapshotterName
	if snapshotter == "" {
		snapshotter = d.name
	}
	_, err := client.Stat(d.WithNamespaceContext(ctx), &snapshots.StatSnapshotRequest{
		Snapshotter: snapshotter,
		Key:         probeKey,
	})
	switch status.Code(err) {
	case codes.OK, codes.NotFound:
		if d.probeHook != nil {
			if err := d.probeHook(ctx); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("stat probe snapshot: %w", err)
	}
}

// PrepareImage asks the remote snapshotter to provide the layers for the image
// and returns read-only views of them. Concurrent calls for the same image are
// coalesced so only one goroutine prepares the views while waiters share the
// resulting lease and increment its reference count. If the snapshotter
// declines the image, it returns an error wrapping ocilayer.ErrNotSupported.
func (d *Driver) PrepareImage(ctx context.Context, req *ocilayer.PrepareRequest) (*ocilayer.PrepareResult, error) {
	if req == nil || req.ImageRef == "" {
		return nil, errors.New("image reference is required")
	}

	var flight *inflightPrep
	d.mu.Lock()
	for {
		if existingFlight, ok := d.inflight[req.ImageRef]; ok {
			d.mu.Unlock()
			select {
			case <-existingFlight.done:
				if existingFlight.err != nil {
					return nil, existingFlight.err
				}
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			d.mu.Lock()
			continue
		}
		if lease, ok := d.leases[req.ImageRef]; ok && !lease.evicted && lease.refCount >= 0 && lease.config != nil {
			if layersExist(lease.layers) {
				if lease.idleTimer != nil {
					lease.idleTimer.Stop()
					lease.idleTimer = nil
				}
				lease.refCount++
				cfgCopy := *lease.config
				res := &ocilayer.PrepareResult{
					ImageDigest:  lease.digest,
					Config:       &cfgCopy,
					LayerDirs:    append([]string(nil), lease.layers...),
					ProviderName: d.name,
				}
				hook := d.warmHitHook
				d.mu.Unlock()
				if hook != nil {
					hook(ctx, CanonicalImageRef(req.ImageRef, res.ImageDigest), req)
				}
				return res, nil
			}
			if lease.idleTimer != nil {
				lease.idleTimer.Stop()
				lease.idleTimer = nil
			}
			slog.WarnContext(ctx, "Cached streaming lease layers missing on disk; re-preparing layers",
				slog.String("provider", d.name),
				slog.String("image", req.ImageRef))
		}
		if d.isDeclinedLocked(req.ImageRef, time.Now()) {
			d.mu.Unlock()
			return nil, fmt.Errorf("previously declined by snapshotter: %w", ocilayer.ErrNotSupported)
		}
		flight = &inflightPrep{done: make(chan struct{})}
		d.inflight[req.ImageRef] = flight
		d.mu.Unlock()
		break
	}

	res, err := d.prepareImageCold(ctx, req)

	d.mu.Lock()
	if errors.Is(err, ocilayer.ErrNotSupported) && d.declineTTL > 0 {
		d.declined[req.ImageRef] = time.Now().Add(d.declineTTL)
	}
	flight.err = err
	delete(d.inflight, req.ImageRef)
	close(flight.done)
	d.mu.Unlock()

	return res, err
}

func layersExist(layers []string) bool {
	if len(layers) == 0 {
		return false
	}
	for _, l := range layers {
		fsPath := filepath.Join(l, "fs")
		if _, lerr := os.Lstat(fsPath); lerr == nil {
			if _, err := os.Stat(fsPath); err != nil {
				return false
			}
		} else if _, err := os.Stat(l); err != nil {
			return false
		}
	}
	return true
}

func (d *Driver) prepareImageCold(ctx context.Context, req *ocilayer.PrepareRequest) (*ocilayer.PrepareResult, error) {
	coldStart := time.Now()
	client, err := d.getClient()
	if err != nil {
		return nil, fmt.Errorf("connecting to snapshotter %s: %w", d.name, err)
	}

	var (
		digest       string
		cfg          *v1.Config
		diffIDs      []string
		layerDigests []string
		resolveDur   time.Duration
	)
	d.mu.Lock()
	var cachedMeta *resolvedImageMeta
	if d.resolvedMeta != nil {
		cachedMeta = d.resolvedMeta[req.ImageRef]
	}
	warmHook := d.warmHitHook
	d.mu.Unlock()

	if cachedMeta != nil && len(cachedMeta.diffIDs) > 0 {
		digest = cachedMeta.digest
		cfg = cachedMeta.config
		diffIDs = append([]string(nil), cachedMeta.diffIDs...)
		layerDigests = append([]string(nil), cachedMeta.layerDigests...)
	} else {
		resolveStart := time.Now()
		digest, cfg, diffIDs, layerDigests, err = d.imageResolver(ctx, req.ImageRef, req.AuthConfig)
		resolveDur = time.Since(resolveStart)
		if err != nil {
			return nil, fmt.Errorf("resolving image %s: %w", req.ImageRef, err)
		}
	}

	criImageRef := CanonicalImageRef(req.ImageRef, digest)

	// Fast path for reconciled leases whose layers are already mounted on host:
	d.mu.Lock()
	if lease, ok := d.leases[req.ImageRef]; ok && !lease.evicted && len(lease.layers) > 0 && layersExist(lease.layers) {
		if lease.idleTimer != nil {
			lease.idleTimer.Stop()
			lease.idleTimer = nil
		}
		lease.config = cfg
		if lease.digest == "" {
			lease.digest = digest
		}
		if d.resolvedMeta == nil {
			d.resolvedMeta = make(map[string]*resolvedImageMeta)
		}
		d.resolvedMeta[req.ImageRef] = &resolvedImageMeta{
			digest:       lease.digest,
			config:       lease.config,
			diffIDs:      append([]string(nil), diffIDs...),
			layerDigests: append([]string(nil), layerDigests...),
		}
		lease.refCount++
		cfgCopy := *lease.config
		res := &ocilayer.PrepareResult{
			ImageDigest:  lease.digest,
			Config:       &cfgCopy,
			LayerDirs:    append([]string(nil), lease.layers...),
			ProviderName: d.name,
		}
		d.mu.Unlock()
		if warmHook != nil {
			warmHook(ctx, criImageRef, req)
		}
		return res, nil
	}
	d.mu.Unlock()

	if len(diffIDs) == 0 {
		return nil, fmt.Errorf("image %s has 0 layers", req.ImageRef)
	}

	chainInfos := ComputeChainIDs(diffIDs)
	imgKey := SanitizePathKey(req.ImageRef)
	imageWorkDir := filepath.Join(d.workDir, imgKey)

	snapshotter := d.snapshotterName
	if snapshotter == "" {
		snapshotter = d.name
	}

	env := &MountEnv{
		Driver:       d,
		Client:       client,
		Snapshotter:  snapshotter,
		ImageRef:     req.ImageRef,
		CanonicalRef: criImageRef,
		ImageDigest:  digest,
		ImageWorkDir: imageWorkDir,
		AuthConfig:   req.AuthConfig,
		ChainInfos:   chainInfos,
		LayerDigests: layerDigests,
	}

	mounted, mountErr := d.layerMounter(ctx, env)
	cleanupOnErr := func() {
		if d.workDirTeardownHook != nil {
			d.workDirTeardownHook(imageWorkDir)
		}
		if mounted != nil {
			nsCtx := d.WithNamespaceContext(context.Background())
			for _, key := range mounted.SnapshotKeys {
				RemoveSnapshot(nsCtx, client, snapshotter, key)
			}
			for i := len(mounted.CommittedKeys) - 1; i >= 0; i-- {
				RemoveSnapshot(nsCtx, client, snapshotter, mounted.CommittedKeys[i])
			}
		}
		_ = os.RemoveAll(imageWorkDir)
	}
	if mountErr != nil {
		cleanupOnErr()
		return nil, mountErr
	}
	if mounted == nil || len(mounted.MountDirs) == 0 {
		cleanupOnErr()
		return nil, fmt.Errorf("provider %s returned 0 layer mounts for %s", d.name, req.ImageRef)
	}

	var (
		listableDur time.Duration
		wrapperDur  time.Duration
		layerDirs   = make([]string, 0, len(mounted.MountDirs))
	)

	for i, mountDir := range mounted.MountDirs {
		if mountDir == "" {
			cleanupOnErr()
			return nil, fmt.Errorf("empty mount directory for layer %d", i)
		}

		listStart := time.Now()
		if err := d.probeListable(ctx, mountDir); err != nil {
			cleanupOnErr()
			return nil, fmt.Errorf("layer %d mount not listable: %w", i, err)
		}
		listableDur += time.Since(listStart)

		wrapStart := time.Now()
		layerDir := filepath.Join(imageWorkDir, fmt.Sprintf("layer-%d", i))
		if err := os.MkdirAll(layerDir, 0o755); err != nil {
			cleanupOnErr()
			return nil, fmt.Errorf("creating layer wrapper directory %s: %w", layerDir, err)
		}

		fsLink := filepath.Join(layerDir, "fs")
		_ = os.Remove(fsLink)
		if err := os.Symlink(mountDir, fsLink); err != nil {
			cleanupOnErr()
			return nil, fmt.Errorf("symlinking layer view %s -> %s: %w", fsLink, mountDir, err)
		}

		finalizedMarker := filepath.Join(layerDir, "finalized")
		if err := os.WriteFile(finalizedMarker, nil, 0o600); err != nil {
			cleanupOnErr()
			return nil, fmt.Errorf("writing finalized marker %s: %w", finalizedMarker, err)
		}
		wrapperDur += time.Since(wrapStart)

		go warmLayerMetadata(mountDir)

		layerDirs = append(layerDirs, layerDir)
	}

	leaseWriteStart := time.Now()
	if err := writeLeaseMetadata(imageWorkDir, &persistedLease{
		ImageRef:     req.ImageRef,
		Digest:       digest,
		Config:       cfg,
		DiffIDs:      diffIDs,
		LayerDigests: layerDigests,
		SnapshotKeys: mounted.SnapshotKeys,
		LayerDirs:    layerDirs,
	}); err != nil {
		cleanupOnErr()
		return nil, fmt.Errorf("writing lease metadata: %w", err)
	}
	wrapperDur += time.Since(leaseWriteStart)

	d.mu.Lock()
	var staleSnapshotKeys []string
	newRefCount := 1
	if prev, ok := d.leases[req.ImageRef]; ok && prev != nil {
		if prev.idleTimer != nil {
			prev.idleTimer.Stop()
			prev.idleTimer = nil
		}
		prev.evicted = true
		if prev.refCount > 0 {
			newRefCount = prev.refCount + 1
		}
		staleSnapshotKeys = append(staleSnapshotKeys, prev.snapshotKeys...)
	}
	d.leases[req.ImageRef] = &imageLease{
		imageRef:      req.ImageRef,
		digest:        digest,
		config:        cfg,
		snapshotKeys:  mounted.SnapshotKeys,
		committedKeys: mounted.CommittedKeys,
		layers:        layerDirs,
		workDir:       imageWorkDir,
		refCount:      newRefCount,
	}
	if d.resolvedMeta == nil {
		d.resolvedMeta = make(map[string]*resolvedImageMeta)
	}
	metaEntry := &resolvedImageMeta{
		digest:       digest,
		config:       cfg,
		diffIDs:      append([]string(nil), diffIDs...),
		layerDigests: append([]string(nil), layerDigests...),
	}
	d.resolvedMeta[req.ImageRef] = metaEntry
	if criImageRef != "" {
		d.resolvedMeta[criImageRef] = metaEntry
	}
	d.mu.Unlock()

	nsCtx := d.WithNamespaceContext(context.Background())
	for _, key := range staleSnapshotKeys {
		RemoveSnapshot(nsCtx, client, snapshotter, key)
	}

	slog.InfoContext(ctx, "PrepareImage timing breakdown",
		slog.String("provider", d.name),
		slog.String("image", req.ImageRef),
		slog.String("digest", digest),
		slog.Int("layers", len(layerDirs)),
		slog.Float64("resolve_ms", float64(resolveDur.Microseconds())/1000.0),
		slog.Float64("stat_ms", float64(mounted.StatDur.Microseconds())/1000.0),
		slog.Float64("prepare_ms", float64(mounted.PrepareDur.Microseconds())/1000.0),
		slog.Float64("view_ms", float64(mounted.ViewDur.Microseconds())/1000.0),
		slog.Float64("listable_ms", float64(listableDur.Microseconds())/1000.0),
		slog.Float64("wrapper_ms", float64(wrapperDur.Microseconds())/1000.0),
		slog.Float64("total_ms", float64(time.Since(coldStart).Microseconds())/1000.0),
	)

	cfgCopy := *cfg
	return &ocilayer.PrepareResult{
		ImageDigest:  digest,
		Config:       &cfgCopy,
		LayerDirs:    layerDirs,
		ProviderName: d.name,
	}, nil
}

const leaseMetadataFile = "lease.json"

type persistedLease struct {
	ImageRef     string     `json:"imageRef"`
	Digest       string     `json:"digest,omitempty"`
	Config       *v1.Config `json:"config,omitempty"`
	DiffIDs      []string   `json:"diffIDs,omitempty"`
	LayerDigests []string   `json:"layerDigests,omitempty"`
	SnapshotKeys []string   `json:"snapshotKeys,omitempty"`
	LayerDirs    []string   `json:"layerDirs,omitempty"`
}

func writeLeaseMetadata(dir string, meta *persistedLease) error {
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, leaseMetadataFile), b, 0o600)
}

func readLeaseMetadata(dir string) (*persistedLease, error) {
	b, err := os.ReadFile(filepath.Join(dir, leaseMetadataFile))
	if err != nil {
		return nil, err
	}
	var meta persistedLease
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (d *Driver) probeListable(ctx context.Context, mountDir string) error {
	if d.listableTimeout <= 0 {
		return nil
	}
	interval := d.listableInterval
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	start := time.Now()
	for {
		entries, err := os.ReadDir(mountDir)
		if err == nil {
			_ = entries
			return nil
		}
		if time.Since(start) > d.listableTimeout {
			return fmt.Errorf("mount %q not listable within %s: %w", mountDir, d.listableTimeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// ReleaseImage decrements the reference count for the image lease. When
// refCount reaches 0, if d.leaseIdleTTL > 0 and !req.BypassTTL the lease and
// its mounted views are kept warm for d.leaseIdleTTL; otherwise the views and
// wrapper directory are removed immediately.
func (d *Driver) ReleaseImage(ctx context.Context, req *ocilayer.ReleaseRequest) error {
	if req == nil || req.ImageRef == "" {
		return nil
	}

	var (
		lease  *imageLease
		flight *inflightPrep
	)
	d.mu.Lock()
	for {
		var ok bool
		lease, ok = d.leases[req.ImageRef]
		if !ok || lease.evicted {
			d.mu.Unlock()
			return nil
		}
		if existingFlight, inflight := d.inflight[req.ImageRef]; inflight {
			d.mu.Unlock()
			select {
			case <-existingFlight.done:
			case <-ctx.Done():
				return ctx.Err()
			}
			d.mu.Lock()
			continue
		}
		if lease.refCount <= 0 {
			if req.BypassTTL && lease.idleTimer != nil {
				flight = d.beginLeaseTeardownLocked(req.ImageRef, lease)
				d.mu.Unlock()
				break
			}
			d.mu.Unlock()
			return nil
		}
		lease.refCount--
		if lease.refCount > 0 {
			d.mu.Unlock()
			return nil
		}
		if d.leaseIdleTTL > 0 && !req.BypassTTL && !d.closed {
			if lease.idleTimer != nil {
				lease.idleTimer.Stop()
			}
			targetLease := lease
			imageRef := req.ImageRef
			if lease.imageRef != "" {
				imageRef = lease.imageRef
			}
			lease.idleTimer = time.AfterFunc(d.leaseIdleTTL, func() {
				d.evictIdleLease(imageRef, targetLease)
			})
			d.mu.Unlock()
			return nil
		}
		flight = d.beginLeaseTeardownLocked(req.ImageRef, lease)
		d.mu.Unlock()
		break
	}

	d.finishLeaseTeardown(ctx, req.ImageRef, lease, flight)
	return nil
}

func (d *Driver) beginLeaseTeardownLocked(imageRef string, lease *imageLease) *inflightPrep {
	lease.evicted = true
	if lease.idleTimer != nil {
		lease.idleTimer.Stop()
		lease.idleTimer = nil
	}
	if d.leases[imageRef] == lease {
		delete(d.leases, imageRef)
	}
	if lease.imageRef != "" && d.leases[lease.imageRef] == lease {
		delete(d.leases, lease.imageRef)
	}
	if lease.digest != "" && d.leases[lease.digest] == lease {
		delete(d.leases, lease.digest)
	}
	if d.inflight == nil {
		d.inflight = make(map[string]*inflightPrep)
	}
	flight := &inflightPrep{done: make(chan struct{})}
	d.inflight[imageRef] = flight
	return flight
}

func (d *Driver) finishLeaseTeardown(ctx context.Context, imageRef string, lease *imageLease, flight *inflightPrep) {
	defer func() {
		d.mu.Lock()
		if d.inflight[imageRef] == flight {
			delete(d.inflight, imageRef)
		}
		close(flight.done)
		d.mu.Unlock()
	}()

	if lease.workDir != "" && d.workDirTeardownHook != nil {
		d.workDirTeardownHook(lease.workDir)
	}

	client, err := d.getClient()
	if err == nil && client != nil {
		snapshotter := d.snapshotterName
		if snapshotter == "" {
			snapshotter = d.name
		}
		nsCtx := d.WithNamespaceContext(ctx)
		for _, key := range lease.snapshotKeys {
			_, delErr := client.Remove(nsCtx, &snapshots.RemoveSnapshotRequest{
				Snapshotter: snapshotter,
				Key:         key,
			})
			if delErr != nil {
				slog.Debug("RemoveSnapshot cleanup notification", "key", key, "error", delErr)
			}
		}
	}

	if lease.workDir != "" {
		_ = os.RemoveAll(lease.workDir)
	}
}

func (d *Driver) evictIdleLease(imageRef string, targetLease *imageLease) {
	d.mu.Lock()
	for {
		if d.closed || targetLease.evicted || targetLease.refCount > 0 || d.leases[imageRef] != targetLease {
			d.mu.Unlock()
			return
		}
		if existingFlight, inflight := d.inflight[imageRef]; inflight {
			d.mu.Unlock()
			<-existingFlight.done
			d.mu.Lock()
			continue
		}
		flight := d.beginLeaseTeardownLocked(imageRef, targetLease)
		d.mu.Unlock()
		d.finishLeaseTeardown(context.Background(), imageRef, targetLease, flight)
		return
	}
}

// ReconcileImages restores active lease tracking and reference counts for
// surviving actor workloads on atelet startup or reconnect, and sweeps orphaned
// wrapper directories and snapshot views not referenced by any active actor.
func (d *Driver) ReconcileImages(ctx context.Context, active []*ocilayer.ActiveImageLayers) error {
	activeWorkDirs := make(map[string]bool)
	activeKeys := make(map[string]bool)

	d.mu.Lock()
	for _, existing := range d.leases {
		if existing != nil && !existing.evicted {
			existing.refCount = 0
		}
	}

	for _, al := range active {
		if al == nil || (al.ImageRef == "" && al.ImageDigest == "") {
			continue
		}
		key := al.ImageRef
		if key == "" {
			key = al.ImageDigest
		}
		activeKeys[key] = true
		if al.ImageDigest != "" {
			activeKeys[al.ImageDigest] = true
		}

		workDir := filepath.Join(d.workDir, SanitizePathKey(key))
		if len(al.LayerDirs) > 0 {
			workDir = filepath.Dir(al.LayerDirs[0])
		}
		activeWorkDirs[workDir] = true
		activeWorkDirs[filepath.Join(d.workDir, SanitizePathKey(key))] = true

		meta, _ := readLeaseMetadata(workDir)
		if meta == nil {
			meta, _ = readLeaseMetadata(filepath.Join(d.workDir, SanitizePathKey(key)))
		}

		existing, ok := d.leases[key]
		if ok && !existing.evicted {
			if existing.idleTimer != nil {
				existing.idleTimer.Stop()
				existing.idleTimer = nil
			}
			existing.refCount += al.RefCount
			if existing.imageRef == "" {
				existing.imageRef = key
			}
			if len(existing.layers) == 0 && len(al.LayerDirs) > 0 {
				existing.layers = append([]string(nil), al.LayerDirs...)
			}
			if existing.digest == "" {
				existing.digest = al.ImageDigest
			}
			if existing.workDir == "" {
				existing.workDir = workDir
			}
		} else {
			existing = &imageLease{
				imageRef: key,
				refCount: al.RefCount,
				digest:   al.ImageDigest,
				layers:   append([]string(nil), al.LayerDirs...),
				workDir:  workDir,
			}
			d.leases[key] = existing
		}
		if meta != nil {
			if existing.config == nil {
				existing.config = meta.Config
			}
			if len(existing.snapshotKeys) == 0 && len(meta.SnapshotKeys) > 0 {
				existing.snapshotKeys = append([]string(nil), meta.SnapshotKeys...)
			}
			if existing.digest == "" {
				existing.digest = meta.Digest
			}
			if len(existing.layers) == 0 && len(meta.LayerDirs) > 0 {
				existing.layers = append([]string(nil), meta.LayerDirs...)
			}
			if len(meta.DiffIDs) > 0 {
				if d.resolvedMeta == nil {
					d.resolvedMeta = make(map[string]*resolvedImageMeta)
				}
				d.resolvedMeta[key] = &resolvedImageMeta{
					digest:       existing.digest,
					config:       existing.config,
					diffIDs:      append([]string(nil), meta.DiffIDs...),
					layerDigests: append([]string(nil), meta.LayerDigests...),
				}
			}
		}
		if al.ImageDigest != "" && al.ImageDigest != key {
			d.leases[al.ImageDigest] = existing
		}
		slog.InfoContext(ctx, "Reconciled active streamed image lease",
			slog.String("provider", d.Name()),
			slog.String("image", key),
			slog.Int("refCount", existing.refCount))
	}

	type orphanTeardown struct {
		key    string
		lease  *imageLease
		flight *inflightPrep
	}
	var orphans []orphanTeardown
	seenLease := make(map[*imageLease]bool)
	for k, l := range d.leases {
		if l == nil || l.evicted || seenLease[l] {
			continue
		}
		seenLease[l] = true
		if l.refCount <= 0 && !activeKeys[k] && !activeKeys[l.imageRef] {
			refKey := l.imageRef
			if refKey == "" {
				refKey = k
			}
			flight := d.beginLeaseTeardownLocked(refKey, l)
			orphans = append(orphans, orphanTeardown{key: refKey, lease: l, flight: flight})
			continue
		}
		if l.workDir != "" {
			activeWorkDirs[l.workDir] = true
		}
	}
	d.mu.Unlock()

	for _, o := range orphans {
		d.finishLeaseTeardown(ctx, o.key, o.lease, o.flight)
	}

	d.sweepOrphanWorkDirs(activeWorkDirs)
	return nil
}

func (d *Driver) sweepOrphanWorkDirs(activeWorkDirs map[string]bool) {
	if d.workDir == "" {
		return
	}
	entries, err := os.ReadDir(d.workDir)
	if err != nil {
		return
	}
	snapshotter := d.snapshotterName
	if snapshotter == "" {
		snapshotter = d.name
	}
	var client snapshots.SnapshotsClient
	nsCtx := d.WithNamespaceContext(context.Background())
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirPath := filepath.Join(d.workDir, entry.Name())
		if activeWorkDirs[dirPath] {
			continue
		}
		if d.workDirTeardownHook != nil {
			d.workDirTeardownHook(dirPath)
		}
		if meta, err := readLeaseMetadata(dirPath); err == nil && meta != nil && len(meta.SnapshotKeys) > 0 {
			if client == nil {
				client, _ = d.getClient()
			}
			if client != nil {
				for _, key := range meta.SnapshotKeys {
					RemoveSnapshot(nsCtx, client, snapshotter, key)
				}
			}
		}
		_ = os.RemoveAll(dirPath)
		slog.Info("Swept orphaned streamed image workdir",
			slog.String("provider", d.Name()),
			slog.String("workDir", dirPath))
	}
}

// Close stops any pending idle lease timers and closes gRPC connections.
func (d *Driver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for _, l := range d.leases {
		if l != nil && l.idleTimer != nil {
			l.idleTimer.Stop()
			l.idleTimer = nil
		}
	}
	if d.conn != nil {
		_ = d.conn.Close()
		d.conn = nil
	}
	return nil
}

func (d *Driver) getClient() (snapshots.SnapshotsClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.snapshotsClient != nil {
		return d.snapshotsClient, nil
	}
	sock := d.SocketPath()
	target := "unix://" + sock
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = d.WithNamespaceContext(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptor),
	)
	if err != nil {
		return nil, err
	}
	d.conn = conn
	d.snapshotsClient = snapshots.NewSnapshotsClient(conn)
	return d.snapshotsClient, nil
}

func warmLayerMetadata(root string) {
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		_, _ = d.Info()
		return nil
	})
}

func (d *Driver) defaultImageResolver(ctx context.Context, refStr string, authConfig *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error) {
	ref, err := name.ParseReference(refStr)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("parsing reference %q: %w", refStr, err)
	}

	img, err := remote.Image(ref, RemoteOptions(ctx, d.keychain, authConfig)...)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("remote.Image: %w", err)
	}

	digest, err := img.Digest()
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("reading image digest: %w", err)
	}

	cfg, err := img.ConfigFile()
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("reading image config: %w", err)
	}

	manifest, err := img.Manifest()
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("reading image manifest: %w", err)
	}

	layerDigests := make([]string, len(manifest.Layers))
	for i, l := range manifest.Layers {
		layerDigests[i] = l.Digest.String()
	}

	diffIDs := make([]string, len(cfg.RootFS.DiffIDs))
	for i, diff := range cfg.RootFS.DiffIDs {
		diffIDs[i] = diff.String()
	}
	return digest.String(), &cfg.Config, diffIDs, layerDigests, nil
}

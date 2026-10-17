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
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/internal/ocilayer"
	"github.com/agent-substrate/substrate/internal/ocilayer/adapter"
	"github.com/agent-substrate/substrate/internal/proto/snapshots"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	// ProviderSpegel identifies the stateless Spegel P2P OCI layer provider.
	ProviderSpegel = "spegel"

	// DefaultSpegelMirrorAddr is the default local Spegel OCI registry mirror HTTP endpoint.
	DefaultSpegelMirrorAddr = "http://127.0.0.1:5000"

	// DefaultSpegelRouterAddr is the default Spegel P2P router/DHT address.
	DefaultSpegelRouterAddr = ":5001"

	// DefaultSpegelBootstrapDNS is the default headless Service DNS domain used
	// to discover cluster Spegel peers in ate-system.
	DefaultSpegelBootstrapDNS = "spegel-bootstrap.ate-system.svc.cluster.local"

	// DefaultSpegelPeerPort is the default registry port on cluster Spegel peers.
	DefaultSpegelPeerPort = 5000

	// DefaultSpegelStoreDir is the default host directory where SpegelDriver
	// stores content-addressed OCI blobs (blobs/sha256/<digest>) and unpacked
	// layer trees (layers/sha256/<chainID>/fs). It sits under
	// /var/lib/ateom-gvisor/streaming so symlinks from layer-<i>/fs resolve
	// inside atelet and ateom-gvisor pods without additional hostPath mounts.
	DefaultSpegelStoreDir = "/var/lib/ateom-gvisor/streaming/spegel-store"

	// DefaultDockerConfigPath is the default Docker config.json path used for
	// credential synchronization.
	DefaultDockerConfigPath = "/run/ate-image-streaming/spegel/docker/config.json"

	// EnvSpegelMirrorAddr overrides the local Spegel HTTP mirror endpoint.
	EnvSpegelMirrorAddr = "SPEGEL_MIRROR_ADDR"

	// EnvSpegelRouterAddr overrides the Spegel P2P router address.
	EnvSpegelRouterAddr = "SPEGEL_ROUTER_ADDR"

	// EnvSpegelBootstrapDNS overrides the headless Service DNS domain for peer discovery.
	EnvSpegelBootstrapDNS = "SPEGEL_BOOTSTRAP_DNS"

	// EnvSpegelUpstreamMirrorAddr specifies an optional secondary local Spegel
	// daemon endpoint (e.g. http://127.0.0.1:5003).
	EnvSpegelUpstreamMirrorAddr = "SPEGEL_UPSTREAM_MIRROR_ADDR"

	// HeaderSpegelExternalRequest marks a peer-to-peer request so the receiving
	// Spegel peer only checks its local blob store and never recursively fans out.
	HeaderSpegelExternalRequest = "X-Spegel-External-Request"

	// HeaderSpegelPeer reports the Spegel peer address that served a blob.
	HeaderSpegelPeer = "X-Spegel-Peer"
)

// BlobFetcherFunc returns a reader for the compressed OCI layer blob at
// layerIndex (with digest layerDigest) from the upstream registry.
type BlobFetcherFunc func(ctx context.Context, imageRef string, auth *ocilayer.AuthConfig, layerIndex int, layerDigest string) (io.ReadCloser, error)

// SpegelConfig configures a SpegelDriver.
type SpegelConfig struct {
	StoreDir                 string
	DockerConfigPath         string
	SpegelMirrorAddr         string
	SpegelRouterAddr         string
	SpegelBootstrapDNS       string
	SpegelPeerPort           int
	SpegelPeers              []string
	SpegelUpstreamMirrorAddr string
	EnableP2PServer          bool
	P2PListenAddr            string
	VerifySpegelReady        bool
	HTTPClient               *http.Client
	UpstreamBlobFetcher      BlobFetcherFunc
	AdapterOpts              []adapter.Option
}

// SpegelStats holds counters exposed by SpegelPeerServer's /stats endpoint.
type SpegelStats struct {
	BlobsCached     int      `json:"blobs_cached"`
	BytesCached     int64    `json:"bytes_cached"`
	LocalHits       int64    `json:"local_hits"`
	PeerHits        int64    `json:"peer_hits"`
	PeerMisses      int64    `json:"peer_misses"`
	P2PRequestsIn   int64    `json:"p2p_requests_in"`
	P2PBytesServed  int64    `json:"p2p_bytes_served"`
	HealthPings     int64    `json:"health_pings"`
	DiscoveredPeers []string `json:"discovered_peers"`
}

// SpegelPeerServer implements a Spegel-compatible stateless P2P OCI registry
// mirror HTTP server backed by the node's content-addressed OCI blob store
// (<StoreDir>/blobs/sha256/<hex>) and DNS/static peer discovery.
type SpegelPeerServer struct {
	storeDir           string
	bootstrapDNS       string
	peerPort           int
	staticPeers        []string
	upstreamMirrorAddr string
	httpClient         *http.Client
	listener           net.Listener
	server             *http.Server

	localHits      atomic.Int64
	peerHits       atomic.Int64
	peerMisses     atomic.Int64
	p2pRequestsIn  atomic.Int64
	p2pBytesServed atomic.Int64
	healthPings    atomic.Int64
}

// NewSpegelPeerServer creates and starts a Spegel-compatible P2P OCI registry
// mirror server on listenAddr.
func NewSpegelPeerServer(listenAddr, storeDir, bootstrapDNS string, peerPort int, staticPeers []string, upstreamMirrorAddr string, httpClient *http.Client) (*SpegelPeerServer, error) {
	if listenAddr == "" {
		listenAddr = ":5000"
	}
	if peerPort <= 0 {
		peerPort = DefaultSpegelPeerPort
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Minute}
	}
	if err := os.MkdirAll(filepath.Join(storeDir, "blobs", "sha256"), 0o755); err != nil {
		return nil, fmt.Errorf("creating Spegel blob store directory: %w", err)
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", listenAddr, err)
	}
	s := &SpegelPeerServer{
		storeDir:           storeDir,
		bootstrapDNS:       strings.TrimSpace(bootstrapDNS),
		peerPort:           peerPort,
		staticPeers:        append([]string(nil), staticPeers...),
		upstreamMirrorAddr: strings.TrimRight(strings.TrimSpace(upstreamMirrorAddr), "/"),
		httpClient:         httpClient,
		listener:           ln,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/v2/", s.handleV2)
	s.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		_ = s.server.Serve(ln)
	}()
	return s, nil
}

// Addr returns the bound TCP address of the Spegel peer server.
func (s *SpegelPeerServer) Addr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Close shuts down the Spegel peer server.
func (s *SpegelPeerServer) Close() error {
	if s == nil || s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

// Stats returns the current P2P mirror counters and on-disk blob inventory.
func (s *SpegelPeerServer) Stats() SpegelStats {
	var blobsCached int
	var bytesCached int64
	entries, err := os.ReadDir(filepath.Join(s.storeDir, "blobs", "sha256"))
	if err == nil {
		for _, e := range entries {
			if e.IsDir() || strings.Contains(e.Name(), ".tmp") {
				continue
			}
			if info, err := e.Info(); err == nil {
				blobsCached++
				bytesCached += info.Size()
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	peers := s.discoverPeers(ctx)
	cancel()
	return SpegelStats{
		BlobsCached:     blobsCached,
		BytesCached:     bytesCached,
		LocalHits:       s.localHits.Load(),
		PeerHits:        s.peerHits.Load(),
		PeerMisses:      s.peerMisses.Load(),
		P2PRequestsIn:   s.p2pRequestsIn.Load(),
		P2PBytesServed:  s.p2pBytesServed.Load(),
		HealthPings:     s.healthPings.Load(),
		DiscoveredPeers: peers,
	}
}

func (s *SpegelPeerServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	s.healthPings.Add(1)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *SpegelPeerServer) handleStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Stats())
}

func (s *SpegelPeerServer) handleV2(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
	if r.URL.Path == "/v2/" || r.URL.Path == "/v2" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idx := strings.LastIndex(r.URL.Path, "/blobs/sha256:")
	if idx < 0 {
		http.NotFound(w, r)
		return
	}
	hexDigest := strings.TrimPrefix(r.URL.Path[idx+len("/blobs/"):], "sha256:")
	if !isValidSHA256Hex(hexDigest) {
		http.Error(w, "invalid digest", http.StatusBadRequest)
		return
	}

	isExternalP2P := strings.EqualFold(r.Header.Get(HeaderSpegelExternalRequest), "true")
	if isExternalP2P {
		s.p2pRequestsIn.Add(1)
	}

	blobPath := filepath.Join(s.storeDir, "blobs", "sha256", hexDigest)
	if f, err := os.Open(blobPath); err == nil {
		defer func() { _ = f.Close() }()
		if info, err := f.Stat(); err == nil && !info.IsDir() {
			s.localHits.Add(1)
			if isExternalP2P && r.Method == http.MethodGet {
				s.p2pBytesServed.Add(info.Size())
			}
			w.Header().Set("Docker-Content-Digest", "sha256:"+hexDigest)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set(HeaderSpegelPeer, s.Addr())
			http.ServeContent(w, r, hexDigest, info.ModTime(), f)
			return
		}
	}

	if isExternalP2P {
		http.NotFound(w, r)
		return
	}

	// Query discovered cluster Spegel peers (via static list + headless DNS).
	peers := s.discoverPeers(r.Context())
	for _, peer := range peers {
		if s.streamFromPeer(w, r, peer, hexDigest) {
			s.peerHits.Add(1)
			return
		}
	}

	// Optional fallback to a secondary local Spegel daemon (e.g. ghcr.io/spegel-org/spegel on :5003).
	if s.upstreamMirrorAddr != "" {
		if s.streamFromUpstreamMirror(w, r, hexDigest) {
			s.peerHits.Add(1)
			return
		}
	}

	s.peerMisses.Add(1)
	http.NotFound(w, r)
}

func (s *SpegelPeerServer) discoverPeers(ctx context.Context) []string {
	seen := make(map[string]struct{})
	localIPs := localInterfaceIPs()
	var out []string
	addPeer := func(hostPort string) {
		hostPort = strings.TrimSpace(hostPort)
		hostPort = strings.TrimPrefix(hostPort, "http://")
		hostPort = strings.TrimPrefix(hostPort, "https://")
		if hostPort == "" {
			return
		}
		h, p, err := net.SplitHostPort(hostPort)
		if err != nil {
			h = hostPort
			p = fmt.Sprintf("%d", s.peerPort)
			hostPort = net.JoinHostPort(h, p)
		}
		if _, isLocal := localIPs[h]; isLocal && (s.listener != nil && strings.HasSuffix(s.listener.Addr().String(), ":"+p)) {
			return
		}
		if _, ok := seen[hostPort]; !ok {
			seen[hostPort] = struct{}{}
			out = append(out, hostPort)
		}
	}

	for _, p := range s.staticPeers {
		addPeer(p)
	}
	if s.bootstrapDNS != "" {
		lookupCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		addrs, err := net.DefaultResolver.LookupHost(lookupCtx, s.bootstrapDNS)
		cancel()
		if err == nil {
			for _, ip := range addrs {
				if _, isLocal := localIPs[ip]; isLocal {
					continue
				}
				addPeer(net.JoinHostPort(ip, fmt.Sprintf("%d", s.peerPort)))
			}
		}
	}
	return out
}

func (s *SpegelPeerServer) streamFromPeer(w http.ResponseWriter, r *http.Request, peerHostPort, hexDigest string) bool {
	peerURL := fmt.Sprintf("http://%s%s", peerHostPort, r.URL.RequestURI())
	probeCtx, cancel := context.WithTimeout(r.Context(), 1500*time.Millisecond)
	headReq, err := http.NewRequestWithContext(probeCtx, http.MethodHead, peerURL, nil)
	if err != nil {
		cancel()
		return false
	}
	headReq.Header.Set(HeaderSpegelExternalRequest, "true")
	headResp, err := s.httpClient.Do(headReq)
	cancel()
	if err != nil {
		return false
	}
	_ = headResp.Body.Close()
	if headResp.StatusCode != http.StatusOK {
		return false
	}

	if r.Method == http.MethodHead {
		w.Header().Set("Docker-Content-Digest", "sha256:"+hexDigest)
		w.Header().Set(HeaderSpegelPeer, peerHostPort)
		if cl := headResp.Header.Get("Content-Length"); cl != "" {
			w.Header().Set("Content-Length", cl)
		}
		w.WriteHeader(http.StatusOK)
		return true
	}

	getReq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, peerURL, nil)
	if err != nil {
		return false
	}
	getReq.Header.Set(HeaderSpegelExternalRequest, "true")
	if rng := r.Header.Get("Range"); rng != "" {
		getReq.Header.Set("Range", rng)
	}
	resp, err := s.httpClient.Do(getReq)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return false
	}
	w.Header().Set("Docker-Content-Digest", "sha256:"+hexDigest)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set(HeaderSpegelPeer, peerHostPort)
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
	slog.InfoContext(r.Context(), "Streamed OCI layer blob from remote Spegel P2P peer",
		slog.String("peer", peerHostPort),
		slog.String("digest", "sha256:"+hexDigest))
	return true
}

func (s *SpegelPeerServer) streamFromUpstreamMirror(w http.ResponseWriter, r *http.Request, hexDigest string) bool {
	targetURL := s.upstreamMirrorAddr + r.URL.RequestURI()
	req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, nil)
	if err != nil {
		return false
	}
	if rng := r.Header.Get("Range"); rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return false
	}
	w.Header().Set("Docker-Content-Digest", "sha256:"+hexDigest)
	w.Header().Set(HeaderSpegelPeer, s.upstreamMirrorAddr)
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}
	w.WriteHeader(resp.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = io.Copy(w, resp.Body)
	}
	return true
}

func localInterfaceIPs() map[string]struct{} {
	ips := map[string]struct{}{
		"127.0.0.1": {},
		"::1":       {},
		"localhost": {},
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ips
	}
	for _, a := range addrs {
		if ipNet, ok := a.(*net.IPNet); ok && ipNet.IP != nil {
			ips[ipNet.IP.String()] = struct{}{}
		}
	}
	return ips
}

// SpegelDriver wraps adapter.Driver with stateless Spegel P2P OCI blob mirror
// lookup, SHA-256 verified blob caching, and content-addressed layer unpacking.
type SpegelDriver struct {
	*adapter.Driver
	dockerSyncer        *adapter.DockerConfigSyncer
	storeDir            string
	spegelMirrorAddr    string
	spegelRouterAddr    string
	spegelBootstrapDNS  string
	verifySpegelReady   bool
	httpClient          *http.Client
	upstreamBlobFetcher BlobFetcherFunc
	peerServer          *SpegelPeerServer

	p2pBlobHits      atomic.Int64
	upstreamBlobMiss atomic.Int64
}

// ResolveSpegelMirrorAddr returns the local Spegel HTTP mirror endpoint from
// flagVal, $SPEGEL_MIRROR_ADDR, or DefaultSpegelMirrorAddr, validating that it
// is a well-formed HTTP(S) URL.
func ResolveSpegelMirrorAddr(flagVal string) (string, error) {
	addr := strings.TrimSpace(flagVal)
	if addr == "" {
		addr = strings.TrimSpace(os.Getenv(EnvSpegelMirrorAddr))
	}
	if addr == "" {
		addr = DefaultSpegelMirrorAddr
	}
	u, err := url.Parse(addr)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("invalid Spegel mirror endpoint %q (expected http:// or https:// URL)", addr)
	}
	return strings.TrimRight(addr, "/"), nil
}

// ResolveSpegelRouterAddr resolves the Spegel P2P router/DHT address from
// flagVal, $SPEGEL_ROUTER_ADDR, or DefaultSpegelRouterAddr.
func ResolveSpegelRouterAddr(flagVal string) (string, error) {
	addr := strings.TrimSpace(flagVal)
	if addr == "" {
		addr = strings.TrimSpace(os.Getenv(EnvSpegelRouterAddr))
	}
	if addr == "" {
		addr = DefaultSpegelRouterAddr
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return "", fmt.Errorf("invalid Spegel router address %q (expected [host]:port)", addr)
	}
	return addr, nil
}

// NewSpegel creates a new Spegel P2P OCILayerService driver.
func NewSpegel(cfg SpegelConfig) (*SpegelDriver, error) {
	mirrorAddr, err := ResolveSpegelMirrorAddr(cfg.SpegelMirrorAddr)
	if err != nil {
		return nil, err
	}
	routerAddr, err := ResolveSpegelRouterAddr(cfg.SpegelRouterAddr)
	if err != nil {
		return nil, err
	}
	bootstrapDNS := strings.TrimSpace(cfg.SpegelBootstrapDNS)
	if bootstrapDNS == "" {
		bootstrapDNS = strings.TrimSpace(os.Getenv(EnvSpegelBootstrapDNS))
	}
	if bootstrapDNS == "" {
		bootstrapDNS = DefaultSpegelBootstrapDNS
	}
	upstreamMirror := strings.TrimSpace(cfg.SpegelUpstreamMirrorAddr)
	if upstreamMirror == "" {
		upstreamMirror = strings.TrimSpace(os.Getenv(EnvSpegelUpstreamMirrorAddr))
	}

	storeDir := strings.TrimSpace(cfg.StoreDir)
	if storeDir == "" {
		storeDir = DefaultSpegelStoreDir
	}
	if err := os.MkdirAll(filepath.Join(storeDir, "blobs", "sha256"), 0o755); err != nil {
		return nil, fmt.Errorf("creating Spegel blob store directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(storeDir, "layers", "sha256"), 0o755); err != nil {
		return nil, fmt.Errorf("creating Spegel layer store directory: %w", err)
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 2 * time.Minute,
		}
	}

	var peerServer *SpegelPeerServer
	if cfg.EnableP2PServer {
		peerServer, err = NewSpegelPeerServer(
			cfg.P2PListenAddr,
			storeDir,
			bootstrapDNS,
			cfg.SpegelPeerPort,
			cfg.SpegelPeers,
			upstreamMirror,
			httpClient,
		)
		if err != nil {
			return nil, fmt.Errorf("starting Spegel P2P server: %w", err)
		}
		if cfg.SpegelMirrorAddr == "" && os.Getenv(EnvSpegelMirrorAddr) == "" {
			_, port, splitErr := net.SplitHostPort(peerServer.Addr())
			if splitErr == nil {
				mirrorAddr = "http://127.0.0.1:" + port
			}
		}
	}

	sd := &SpegelDriver{
		dockerSyncer:        adapter.NewDockerConfigSyncer(cfg.DockerConfigPath),
		storeDir:            storeDir,
		spegelMirrorAddr:    mirrorAddr,
		spegelRouterAddr:    routerAddr,
		spegelBootstrapDNS:  bootstrapDNS,
		verifySpegelReady:   cfg.VerifySpegelReady,
		httpClient:          httpClient,
		upstreamBlobFetcher: cfg.UpstreamBlobFetcher,
		peerServer:          peerServer,
	}

	opts := append([]adapter.Option{
		adapter.WithSnapshotsClient(&noopSnapshotsClient{}),
		adapter.WithLayerMounter(sd.mountLayers),
		adapter.WithWarmHitHook(sd.onWarmHit),
		adapter.WithProbeHook(sd.probeSpegel),
	}, cfg.AdapterOpts...)

	base, err := adapter.NewDriver(ProviderSpegel, "", ProviderSpegel, opts...)
	if err != nil {
		if peerServer != nil {
			_ = peerServer.Close()
		}
		return nil, err
	}
	sd.Driver = base
	if sd.upstreamBlobFetcher == nil {
		sd.upstreamBlobFetcher = sd.defaultUpstreamBlobFetcher
	}
	return sd, nil
}

// Close closes the Spegel driver and any embedded P2P server.
func (sd *SpegelDriver) Close() error {
	if sd.peerServer != nil {
		_ = sd.peerServer.Close()
	}
	return sd.Driver.Close()
}

// SpegelMirrorAddr returns the configured local Spegel HTTP mirror endpoint.
func (sd *SpegelDriver) SpegelMirrorAddr() string {
	return sd.spegelMirrorAddr
}

// SpegelRouterAddr returns the configured Spegel P2P router/DHT address.
func (sd *SpegelDriver) SpegelRouterAddr() string {
	return sd.spegelRouterAddr
}

// SpegelBootstrapDNS returns the configured headless Service DNS domain for peer discovery.
func (sd *SpegelDriver) SpegelBootstrapDNS() string {
	return sd.spegelBootstrapDNS
}

// StoreDir returns the content-addressed OCI blob and unpacked layer store root.
func (sd *SpegelDriver) StoreDir() string {
	return sd.storeDir
}

// PeerServer returns the embedded SpegelPeerServer, if enabled.
func (sd *SpegelDriver) PeerServer() *SpegelPeerServer {
	return sd.peerServer
}

// P2PBlobHits returns the number of OCI layer blobs fetched via the Spegel P2P mirror.
func (sd *SpegelDriver) P2PBlobHits() int64 {
	return sd.p2pBlobHits.Load()
}

// UpstreamBlobMisses returns the number of OCI layer blobs fetched from the upstream registry on a P2P miss.
func (sd *SpegelDriver) UpstreamBlobMisses() int64 {
	return sd.upstreamBlobMiss.Load()
}

// PingSpegelMirror probes the local Spegel /healthz endpoint to verify the P2P
// mirror is ready.
func (sd *SpegelDriver) PingSpegelMirror(ctx context.Context) error {
	healthURL := sd.spegelMirrorAddr + "/healthz"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return fmt.Errorf("creating Spegel healthz request %q: %w", healthURL, err)
	}
	resp, err := sd.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("probing Spegel mirror at %q: %w", healthURL, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Spegel mirror healthz %q returned HTTP %d", healthURL, resp.StatusCode)
	}
	return nil
}

func (sd *SpegelDriver) probeSpegel(ctx context.Context) error {
	if !sd.verifySpegelReady {
		return nil
	}
	if err := sd.PingSpegelMirror(ctx); err != nil {
		slog.WarnContext(ctx, "Spegel mirror healthz probe failed",
			slog.String("mirrorAddr", sd.spegelMirrorAddr),
			slog.Any("err", err))
		return err
	}
	return nil
}

func (sd *SpegelDriver) onWarmHit(ctx context.Context, _ string, req *ocilayer.PrepareRequest) {
	if req == nil {
		return
	}
	sd.dockerSyncer.Sync(ctx, sd.Driver.Keychain(), req.ImageRef, req.AuthConfig, false)
}

func (sd *SpegelDriver) mountLayers(ctx context.Context, env *adapter.MountEnv) (*adapter.MountedLayers, error) {
	if sd.verifySpegelReady {
		pingCtx, cancel := context.WithTimeout(ctx, adapter.DefaultProbeTimeout)
		err := sd.PingSpegelMirror(pingCtx)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("spegel P2P mirror unavailable (%s): %w", sd.spegelMirrorAddr, err)
		}
	}

	sd.dockerSyncer.Sync(ctx, sd.Driver.Keychain(), env.ImageRef, env.AuthConfig, true)

	registryHost, repoPath, err := parseRegistryAndRepo(env.ImageRef)
	if err != nil {
		return nil, err
	}

	n := len(env.ChainInfos)
	res := &adapter.MountedLayers{
		MountDirs: make([]string, n),
	}

	type layerOutcome struct {
		mountDir   string
		statDur    time.Duration
		prepareDur time.Duration
		err        error
	}
	outcomes := make([]layerOutcome, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i, c := range env.ChainInfos {
		go func(i int, c adapter.ChainInfo) {
			defer wg.Done()

			var layerDigest string
			if i < len(env.LayerDigests) {
				layerDigest = env.LayerDigests[i]
			}
			if layerDigest == "" {
				layerDigest = c.ChainID
			}

			chainHex := stripSHA256Prefix(c.ChainID)
			if !isValidSHA256Hex(chainHex) {
				outcomes[i].err = fmt.Errorf("layer %d has invalid chainID %q", i, c.ChainID)
				return
			}
			blobHex := stripSHA256Prefix(layerDigest)
			if !isValidSHA256Hex(blobHex) {
				outcomes[i].err = fmt.Errorf("layer %d has invalid digest %q", i, layerDigest)
				return
			}

			layerDir := filepath.Join(sd.storeDir, "layers", "sha256", chainHex)
			layerFSDir := filepath.Join(layerDir, "fs")
			finalizedMarker := filepath.Join(layerDir, "finalized")

			lk := env.Driver.AcquireChainLock(c.ChainID)
			lk.Lock()

			statStart := time.Now()
			alreadyUnpacked := isFinalizedLayerDir(layerFSDir, finalizedMarker)
			outcomes[i].statDur = time.Since(statStart)

			var prepErr error
			if !alreadyUnpacked {
				prepStart := time.Now()
				prepErr = sd.materializeSpegelLayer(ctx, env.ImageRef, registryHost, repoPath, env.AuthConfig, i, blobHex, layerDir, layerFSDir, finalizedMarker)
				outcomes[i].prepareDur = time.Since(prepStart)
			}
			lk.Unlock()

			if prepErr != nil {
				outcomes[i].err = fmt.Errorf("layer %d (%s): %w", i, c.ChainID, prepErr)
				return
			}
			outcomes[i].mountDir = layerFSDir
		}(i, c)
	}
	wg.Wait()

	var maxPrepDur time.Duration
	for i := 0; i < n; i++ {
		res.StatDur += outcomes[i].statDur
		if outcomes[i].prepareDur > maxPrepDur {
			maxPrepDur = outcomes[i].prepareDur
		}
		if outcomes[i].err != nil {
			return res, outcomes[i].err
		}
		res.MountDirs[i] = outcomes[i].mountDir
	}
	res.PrepareDur = maxPrepDur

	return res, nil
}

func (sd *SpegelDriver) materializeSpegelLayer(ctx context.Context, imageRef, registryHost, repoPath string, auth *ocilayer.AuthConfig, layerIndex int, blobHex, layerDir, layerFSDir, finalizedMarker string) error {
	blobPath, sourcePeer, err := sd.ensureBlobInStore(ctx, imageRef, registryHost, repoPath, auth, layerIndex, blobHex)
	if err != nil {
		return err
	}

	f, err := os.Open(blobPath)
	if err != nil {
		return fmt.Errorf("opening cached blob %s: %w", blobPath, err)
	}
	defer func() { _ = f.Close() }()

	tarRC, err := openDecompressedTarStream(f)
	if err != nil {
		return fmt.Errorf("decompressing blob sha256:%s: %w", blobHex, err)
	}
	defer func() { _ = tarRC.Close() }()

	stagingDir := fmt.Sprintf("%s.tmp-%x", layerDir, time.Now().UnixNano())
	stagingFS := filepath.Join(stagingDir, "fs")
	if err := os.MkdirAll(stagingFS, 0o755); err != nil {
		return fmt.Errorf("creating staging layer directory %s: %w", stagingFS, err)
	}
	defer func() { _ = os.RemoveAll(stagingDir) }()

	if err := adapter.UnpackLayerTarToDir(ctx, tarRC, stagingFS); err != nil {
		return fmt.Errorf("unpacking layer %d (sha256:%s): %w", layerIndex, blobHex, err)
	}

	if err := os.WriteFile(filepath.Join(stagingDir, "finalized"), nil, 0o600); err != nil {
		return fmt.Errorf("writing staging finalized marker: %w", err)
	}

	_ = os.RemoveAll(layerDir)
	if err := os.Rename(stagingDir, layerDir); err != nil {
		return fmt.Errorf("committing unpacked layer directory %s: %w", layerDir, err)
	}

	slog.InfoContext(ctx, "Materialized Spegel OCI layer",
		slog.Int("layerIndex", layerIndex),
		slog.String("blobDigest", "sha256:"+blobHex),
		slog.String("source", sourcePeer),
		slog.String("layerFSDir", layerFSDir))
	return nil
}

func (sd *SpegelDriver) ensureBlobInStore(ctx context.Context, imageRef, registryHost, repoPath string, auth *ocilayer.AuthConfig, layerIndex int, blobHex string) (string, string, error) {
	blobPath := filepath.Join(sd.storeDir, "blobs", "sha256", blobHex)
	if info, err := os.Stat(blobPath); err == nil && !info.IsDir() && info.Size() > 0 {
		return blobPath, "local-store", nil
	}

	// 1. Query the Spegel P2P mirror first: GET <mirror>/v2/<repo>/blobs/sha256:<hex>?ns=<registry>
	mirrorURL := fmt.Sprintf("%s/v2/%s/blobs/sha256:%s?ns=%s",
		sd.spegelMirrorAddr, strings.TrimPrefix(repoPath, "/"), blobHex, url.QueryEscape(registryHost))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mirrorURL, nil)
	if err == nil {
		resp, doErr := sd.httpClient.Do(req)
		if doErr == nil {
			if resp.StatusCode == http.StatusOK {
				peer := resp.Header.Get(HeaderSpegelPeer)
				if peer == "" {
					peer = sd.spegelMirrorAddr
				}
				writeErr := writeVerifiedBlobAtomically(resp.Body, blobPath, blobHex)
				_ = resp.Body.Close()
				if writeErr == nil {
					sd.p2pBlobHits.Add(1)
					return blobPath, "spegel-p2p:" + peer, nil
				}
				slog.WarnContext(ctx, "Spegel mirror blob failed SHA-256 verification; falling back to upstream registry",
					slog.String("digest", "sha256:"+blobHex),
					slog.Any("err", writeErr))
			} else {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}
	}

	// 2. Cluster-cold miss: pull from the upstream OCI registry and ingest into
	// the local Spegel OCI blob store so this node advertises it to all peers.
	rc, err := sd.upstreamBlobFetcher(ctx, imageRef, auth, layerIndex, "sha256:"+blobHex)
	if err != nil {
		return "", "", fmt.Errorf("fetching blob sha256:%s from upstream registry: %w", blobHex, err)
	}
	defer func() { _ = rc.Close() }()

	if err := writeVerifiedBlobAtomically(rc, blobPath, blobHex); err != nil {
		return "", "", fmt.Errorf("storing upstream blob sha256:%s: %w", blobHex, err)
	}
	sd.upstreamBlobMiss.Add(1)
	return blobPath, "upstream:" + registryHost, nil
}

func (sd *SpegelDriver) defaultUpstreamBlobFetcher(ctx context.Context, imageRef string, authConfig *ocilayer.AuthConfig, layerIndex int, layerDigest string) (io.ReadCloser, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return nil, fmt.Errorf("parsing image reference %q: %w", imageRef, err)
	}
	remoteOpts := adapter.RemoteOptions(ctx, sd.Driver.Keychain(), authConfig)
	if strings.HasPrefix(layerDigest, "sha256:") {
		digestRef, dErr := name.NewDigest(fmt.Sprintf("%s@%s", ref.Context().Name(), layerDigest))
		if dErr == nil {
			layer, lErr := remote.Layer(digestRef, remoteOpts...)
			if lErr == nil {
				return layer.Compressed()
			}
		}
	}
	img, err := remote.Image(ref, remoteOpts...)
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
	return layers[layerIndex].Compressed()
}

func writeVerifiedBlobAtomically(r io.Reader, destPath, expectedHex string) error {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}
	tmpPath := fmt.Sprintf("%s.tmp-%x", destPath, time.Now().UnixNano())
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		_ = f.Close()
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, h), r); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	gotHex := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(gotHex, expectedHex) {
		return fmt.Errorf("blob digest mismatch: got sha256:%s, want sha256:%s", gotHex, expectedHex)
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func openDecompressedTarStream(r io.Reader) (io.ReadCloser, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	magic, err := br.Peek(4)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, bufio.ErrBufferFull) {
		return nil, err
	}
	// Gzip magic: 0x1f 0x8b
	if len(magic) >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			return nil, err
		}
		return gz, nil
	}
	// Zstd magic: 0x28 0xb5 0x2f 0xfd
	if len(magic) >= 4 && magic[0] == 0x28 && magic[1] == 0xb5 && magic[2] == 0x2f && magic[3] == 0xfd {
		zr, err := zstd.NewReader(br)
		if err != nil {
			return nil, err
		}
		return zr.IOReadCloser(), nil
	}
	return io.NopCloser(br), nil
}

func parseRegistryAndRepo(imageRef string) (string, string, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return "", "", fmt.Errorf("parsing image reference %q: %w", imageRef, err)
	}
	return ref.Context().RegistryStr(), ref.Context().RepositoryStr(), nil
}

func stripSHA256Prefix(d string) string {
	return strings.TrimPrefix(strings.TrimSpace(d), "sha256:")
}

func isValidSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func isFinalizedLayerDir(layerFSDir, finalizedMarker string) bool {
	if _, err := os.Stat(finalizedMarker); err != nil {
		return false
	}
	info, err := os.Stat(layerFSDir)
	return err == nil && info.IsDir()
}

type noopSnapshotsClient struct {
	snapshots.SnapshotsClient
}

func (n *noopSnapshotsClient) Stat(_ context.Context, _ *snapshots.StatSnapshotRequest, _ ...grpc.CallOption) (*snapshots.StatSnapshotResponse, error) {
	return nil, status.Error(codes.NotFound, "snapshot not found")
}

func (n *noopSnapshotsClient) Remove(_ context.Context, _ *snapshots.RemoveSnapshotRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

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
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/agent-substrate/substrate/internal/ocilayer"
	"github.com/agent-substrate/substrate/internal/ocilayer/adapter"
	ocilayerpb "github.com/agent-substrate/substrate/internal/proto/ocilayer"
	"github.com/agent-substrate/substrate/internal/proto/snapshots"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeNydusSnapshotsClient struct {
	mu          sync.Mutex
	committed   map[string]bool
	activeDir   string
	viewMounts  []*snapshots.Mount
	prepareReqs []*snapshots.PrepareSnapshotRequest
	commitReqs  []*snapshots.CommitSnapshotRequest
	viewReqs    []*snapshots.ViewSnapshotRequest
}

func (f *fakeNydusSnapshotsClient) Prepare(_ context.Context, in *snapshots.PrepareSnapshotRequest, _ ...grpc.CallOption) (*snapshots.PrepareSnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepareReqs = append(f.prepareReqs, in)

	targetChainID := in.GetLabels()["containerd.io/snapshot.ref"]
	if in.GetLabels()[LabelNydusBlob] == "true" && in.GetLabels()[LabelNydusBootstrap] != "true" {
		if targetChainID != "" {
			f.committed[targetChainID] = true
		}
		return nil, status.Error(codes.AlreadyExists, "nydus data blob registered")
	}

	return &snapshots.PrepareSnapshotResponse{
		Mounts: []*snapshots.Mount{
			{
				Type:    "bind",
				Source:  f.activeDir,
				Options: []string{"rw", "rbind"},
			},
		},
	}, nil
}

func (f *fakeNydusSnapshotsClient) View(_ context.Context, in *snapshots.ViewSnapshotRequest, _ ...grpc.CallOption) (*snapshots.ViewSnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.viewReqs = append(f.viewReqs, in)
	return &snapshots.ViewSnapshotResponse{
		Mounts: f.viewMounts,
	}, nil
}

func (f *fakeNydusSnapshotsClient) Mounts(_ context.Context, _ *snapshots.MountsRequest, _ ...grpc.CallOption) (*snapshots.MountsResponse, error) {
	return &snapshots.MountsResponse{}, nil
}

func (f *fakeNydusSnapshotsClient) Commit(_ context.Context, in *snapshots.CommitSnapshotRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitReqs = append(f.commitReqs, in)
	f.committed[in.GetName()] = true
	return &emptypb.Empty{}, nil
}

func (f *fakeNydusSnapshotsClient) Remove(_ context.Context, _ *snapshots.RemoveSnapshotRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeNydusSnapshotsClient) Stat(_ context.Context, in *snapshots.StatSnapshotRequest, _ ...grpc.CallOption) (*snapshots.StatSnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.committed[in.GetKey()] {
		return &snapshots.StatSnapshotResponse{}, nil
	}
	return nil, status.Error(codes.NotFound, "not found")
}

func startTestNydusDragonflyServer(t *testing.T, driver *NydusDragonflyDriver) *ocilayer.GRPCClient {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "ocilayer.sock")
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := grpc.NewServer()
	ocilayerpb.RegisterOCILayerServiceServer(srv, adapter.NewServer(driver.Driver))
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
		_ = driver.Close()
	})
	client, err := ocilayer.NewGRPCClient(sockPath)
	if err != nil {
		t.Fatalf("NewGRPCClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func makeTestTar(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, content := range files {
		hdr := &tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(content)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader(%q): %v", name, err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatalf("Write(%q): %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tw.Close: %v", err)
	}
	return buf.Bytes()
}

func TestVerifyKernelEROFSSupportNoFallback(t *testing.T) {
	tmpDir := t.TempDir()
	procNoEROFS := filepath.Join(tmpDir, "filesystems-cos")
	if err := os.WriteFile(procNoEROFS, []byte("nodev\toverlay\nnodev\tfuse\n\text4\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cachefilesDev := filepath.Join(tmpDir, "cachefiles")
	if err := os.WriteFile(cachefilesDev, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	err := VerifyKernelEROFSSupport(procNoEROFS, cachefilesDev)
	if err == nil || !strings.Contains(err.Error(), "fusedev fallback is disabled") {
		t.Fatalf("VerifyKernelEROFSSupport without erofs = %v, want explicit fusedev fallback disabled error", err)
	}

	procWithEROFS := filepath.Join(tmpDir, "filesystems-ubuntu")
	if err := os.WriteFile(procWithEROFS, []byte("nodev\toverlay\n\terofs\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err = VerifyKernelEROFSSupport(procWithEROFS, filepath.Join(tmpDir, "missing-cachefiles"))
	if err == nil || !strings.Contains(err.Error(), "fusedev fallback is disabled") {
		t.Fatalf("VerifyKernelEROFSSupport without /dev/cachefiles = %v, want explicit fusedev fallback disabled error", err)
	}

	if err := VerifyKernelEROFSSupport(procWithEROFS, cachefilesDev); err != nil {
		t.Fatalf("VerifyKernelEROFSSupport with erofs and cachefiles failed: %v", err)
	}
}

func TestResolveDragonflyEndpoints(t *testing.T) {
	t.Setenv(EnvDragonflyProxyAddr, "")
	t.Setenv(EnvDragonflySchedulerAddr, "")
	t.Setenv(EnvDragonflyManagerAddr, "")

	proxyAddr, err := ResolveDragonflyProxyAddr("")
	if err != nil || proxyAddr != DefaultDragonflyProxyAddr {
		t.Fatalf("default ResolveDragonflyProxyAddr = (%q, %v), want (%q, nil)", proxyAddr, err, DefaultDragonflyProxyAddr)
	}

	schedAddr, err := ResolveDragonflyHostPort("", EnvDragonflySchedulerAddr, DefaultDragonflySchedulerAddr, "scheduler")
	if err != nil || schedAddr != DefaultDragonflySchedulerAddr {
		t.Fatalf("default scheduler = (%q, %v), want (%q, nil)", schedAddr, err, DefaultDragonflySchedulerAddr)
	}

	mgrAddr, err := ResolveDragonflyHostPort("", EnvDragonflyManagerAddr, DefaultDragonflyManagerAddr, "manager")
	if err != nil || mgrAddr != DefaultDragonflyManagerAddr {
		t.Fatalf("default manager = (%q, %v), want (%q, nil)", mgrAddr, err, DefaultDragonflyManagerAddr)
	}

	t.Setenv(EnvDragonflyProxyAddr, "http://127.0.0.1:65001/")
	t.Setenv(EnvDragonflySchedulerAddr, "127.0.0.1:8002")
	t.Setenv(EnvDragonflyManagerAddr, "127.0.0.1:65003")

	proxyAddr, err = ResolveDragonflyProxyAddr("")
	if err != nil || proxyAddr != "http://127.0.0.1:65001" {
		t.Fatalf("env ResolveDragonflyProxyAddr = (%q, %v), want (http://127.0.0.1:65001, nil)", proxyAddr, err)
	}
	schedAddr, err = ResolveDragonflyHostPort("", EnvDragonflySchedulerAddr, DefaultDragonflySchedulerAddr, "scheduler")
	if err != nil || schedAddr != "127.0.0.1:8002" {
		t.Fatalf("env scheduler = (%q, %v), want (127.0.0.1:8002, nil)", schedAddr, err)
	}
	mgrAddr, err = ResolveDragonflyHostPort("", EnvDragonflyManagerAddr, DefaultDragonflyManagerAddr, "manager")
	if err != nil || mgrAddr != "127.0.0.1:65003" {
		t.Fatalf("env manager = (%q, %v), want (127.0.0.1:65003, nil)", mgrAddr, err)
	}

	if _, err := ResolveDragonflyProxyAddr("not-a-url"); err == nil {
		t.Fatal("expected error for invalid Dragonfly proxy URL")
	}
	if _, err := ResolveDragonflyHostPort("invalid-host-port", "", DefaultDragonflySchedulerAddr, "scheduler"); err == nil {
		t.Fatal("expected error for invalid Dragonfly scheduler host:port")
	}
}

func TestPingDragonflyProxyAndCanProvide(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	var proxyHealthy atomic.Bool
	proxyHealthy.Store(true)

	dfServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/server/ping" {
			if proxyHealthy.Load() {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("OK"))
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		http.NotFound(w, r)
	}))
	defer dfServer.Close()

	snapClient := &fakeNydusSnapshotsClient{
		committed: make(map[string]bool),
		activeDir: tmpDir,
	}

	driver, err := NewNydusDragonfly(NydusDragonflyConfig{
		SkipKernelCheck:        true,
		DragonflyProxyAddr:     dfServer.URL,
		DragonflySchedulerAddr: "127.0.0.1:8002",
		DragonflyManagerAddr:   "127.0.0.1:65003",
		VerifyDragonflyPing:    true,
		HTTPClient:             dfServer.Client(),
		AdapterOpts: []adapter.Option{
			adapter.WithWorkDir(filepath.Join(tmpDir, "work")),
			adapter.WithSnapshotsClient(snapClient),
		},
	})
	if err != nil {
		t.Fatalf("NewNydusDragonfly: %v", err)
	}

	client := startTestNydusDragonflyServer(t, driver)
	req := &ocilayer.PrepareRequest{
		ImageRef: "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox:nydus-v6",
	}

	canProvide, err := client.CanProvide(ctx, req)
	if err != nil || !canProvide {
		t.Fatalf("CanProvide with healthy dfdaemon = (%v, %v), want (true, nil)", canProvide, err)
	}

	proxyHealthy.Store(false)
	canProvide, err = client.CanProvide(ctx, req)
	if err != nil || canProvide {
		t.Fatalf("CanProvide with unhealthy dfdaemon = (%v, %v), want (false, nil)", canProvide, err)
	}
}

// fakeDragonflyScheduler coordinates cross-node P2P piece location between
// simulated node dfdaemons in TestNydusDragonflyCrossNodeP2PFlow.
type fakeDragonflyScheduler struct {
	mu         sync.Mutex
	piecePeers map[string]string // pieceKey (path + "|" + range) -> peerURL
}

func (s *fakeDragonflyScheduler) RegisterPiece(pieceKey, peerURL string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.piecePeers == nil {
		s.piecePeers = make(map[string]string)
	}
	s.piecePeers[pieceKey] = peerURL
}

func (s *fakeDragonflyScheduler) LookupPeer(pieceKey string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	peer, ok := s.piecePeers[pieceKey]
	return peer, ok
}

type fakeNodeDfdaemon struct {
	nodeName       string
	scheduler      *fakeDragonflyScheduler
	upstreamURL    string
	httpClient     *http.Client
	server         *httptest.Server
	mu             sync.Mutex
	localPieces    map[string][]byte
	localHits      int
	peerHits       int
	upstreamMisses int
}

func newFakeNodeDfdaemon(t *testing.T, nodeName string, scheduler *fakeDragonflyScheduler, upstreamURL string) *fakeNodeDfdaemon {
	t.Helper()
	d := &fakeNodeDfdaemon{
		nodeName:    nodeName,
		scheduler:   scheduler,
		upstreamURL: upstreamURL,
		httpClient:  &http.Client{},
		localPieces: make(map[string][]byte),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/server/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/peer/piece", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		d.mu.Lock()
		data, ok := d.localPieces[key]
		d.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data)
	})
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		pieceKey := r.URL.Path + "|" + rangeHdr

		// 1. Check same-node local dfdaemon piece cache.
		d.mu.Lock()
		if data, ok := d.localPieces[pieceKey]; ok {
			d.localHits++
			d.mu.Unlock()
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data)
			return
		}
		d.mu.Unlock()

		// 2. Ask Dragonfly Scheduler if a peer node already cached this piece.
		if peerURL, ok := d.scheduler.LookupPeer(pieceKey); ok && peerURL != d.server.URL {
			peerReqURL := fmt.Sprintf("%s/peer/piece?key=%s", peerURL, pieceKey)
			resp, err := d.httpClient.Get(peerReqURL)
			if err == nil && resp.StatusCode == http.StatusPartialContent {
				data, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				d.mu.Lock()
				d.localPieces[pieceKey] = data
				d.peerHits++
				d.mu.Unlock()
				d.scheduler.RegisterPiece(pieceKey, d.server.URL)
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(data)
				return
			}
			if resp != nil {
				_ = resp.Body.Close()
			}
		}

		// 3. Cluster-wide cold miss: fetch back-to-source from upstream registry.
		upReq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, d.upstreamURL+r.URL.Path, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if rangeHdr != "" {
			upReq.Header.Set("Range", rangeHdr)
		}
		resp, err := d.httpClient.Do(upReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		d.mu.Lock()
		d.localPieces[pieceKey] = data
		d.upstreamMisses++
		d.mu.Unlock()
		d.scheduler.RegisterPiece(pieceKey, d.server.URL)

		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(data)
	})
	d.server = httptest.NewServer(mux)
	t.Cleanup(func() { d.server.Close() })
	return d
}

func TestNydusDragonflyCrossNodeP2PFlow(t *testing.T) {
	ctx := context.Background()

	// Simulated upstream GAR serving a Nydus RAFS v6 data blob.
	var upstreamHits atomic.Int32
	blobDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	bootDigest := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	diffIDs := []string{
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	chunkPayload := "RAFS-V6-1MB-CHUNK-PAYLOAD-FROM-GAR"

	upstreamGAR := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte(chunkPayload))
	}))
	defer upstreamGAR.Close()

	scheduler := &fakeDragonflyScheduler{}
	dfNodeA := newFakeNodeDfdaemon(t, "node-a", scheduler, upstreamGAR.URL)
	dfNodeB := newFakeNodeDfdaemon(t, "node-b", scheduler, upstreamGAR.URL)

	bootstrapTar := makeTestTar(t, map[string]string{
		"image/image.boot": "rafs-v6-erofs-superblock",
	})

	newNodeDriver := func(t *testing.T, nodeDf *fakeNodeDfdaemon) (*NydusDragonflyDriver, *ocilayer.GRPCClient) {
		t.Helper()
		tmpDir := t.TempDir()
		bootstrapActiveDir := filepath.Join(tmpDir, "nydus-bootstrap-active")
		if err := os.MkdirAll(bootstrapActiveDir, 0o755); err != nil {
			t.Fatal(err)
		}
		snapClient := &fakeNydusSnapshotsClient{
			committed: make(map[string]bool),
			activeDir: bootstrapActiveDir,
			viewMounts: []*snapshots.Mount{
				{
					Type:    "erofs",
					Source:  "erofs",
					Options: []string{"ro", "fsid=nydus-fsid-1", "domain_id=nydus-domain-1"},
				},
			},
		}
		driver, err := NewNydusDragonfly(NydusDragonflyConfig{
			SkipKernelCheck:        true,
			DockerConfigPath:       filepath.Join(tmpDir, "docker", "config.json"),
			DragonflyProxyAddr:     nodeDf.server.URL,
			DragonflySchedulerAddr: "127.0.0.1:8002",
			DragonflyManagerAddr:   "127.0.0.1:65003",
			VerifyDragonflyPing:    true,
			HTTPClient:             nodeDf.server.Client(),
			LayerAnnotations: func(_ context.Context, _ string, _ *ocilayer.AuthConfig) ([]map[string]string, error) {
				return []map[string]string{
					{
						LabelNydusBlob:      "true",
						LabelNydusFSVersion: "6",
					},
					{
						LabelNydusBootstrap: "true",
						LabelNydusFSVersion: "6",
					},
				}, nil
			},
			LayerReader: func(_ context.Context, _ string, _ *ocilayer.AuthConfig, _ int, _ string) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(bootstrapTar)), nil
			},
			MountFS: func(_ string, targetDir, fstype string, _ uintptr, _ string) error {
				if fstype != "erofs" {
					return fmt.Errorf("unexpected fstype %q", fstype)
				}
				// Simulate nydusd on this node faulting in a 1 MB RAFS v6 chunk
				// through its local dfdaemon proxy during demand-paging.
				blobPath := "/v2/kuiyue-gke-dev/repo/sandbox/blobs/" + blobDigest
				req, err := http.NewRequest(http.MethodGet, nodeDf.server.URL+blobPath, nil)
				if err != nil {
					return err
				}
				req.Header.Set("Range", "bytes=0-1048575")
				resp, err := nodeDf.server.Client().Do(req)
				if err != nil {
					return err
				}
				data, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				return os.WriteFile(filepath.Join(targetDir, "chunk.bin"), data, 0o644)
			},
			UnmountFS: func(_ string, _ int) error { return nil },
			StatFS:    func(_ string) (int64, error) { return EROFSSuperMagic, nil },
			AdapterOpts: []adapter.Option{
				adapter.WithWorkDir(filepath.Join(tmpDir, "work")),
				adapter.WithSnapshotsClient(snapClient),
				adapter.WithLeaseIdleTTL(0),
				adapter.WithImageResolver(func(_ context.Context, _ string, _ *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error) {
					return "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
						&v1.Config{Entrypoint: []string{"/ko-app/sandbox"}},
						diffIDs,
						[]string{blobDigest, bootDigest},
						nil
				}),
			},
		})
		if err != nil {
			t.Fatalf("NewNydusDragonfly(%s): %v", nodeDf.nodeName, err)
		}
		return driver, startTestNydusDragonflyServer(t, driver)
	}

	_, clientA := newNodeDriver(t, dfNodeA)
	_, clientB := newNodeDriver(t, dfNodeB)

	prepReq := &ocilayer.PrepareRequest{
		ImageRef: "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox:nydus-v6",
		AuthConfig: &ocilayer.AuthConfig{
			Username: "oauth2accesstoken",
			Password: "token-value",
		},
	}

	// Step 1: Cold PrepareImage + demand-paging on Node A -> pulls from upstream GAR and registers piece with Scheduler.
	resA, err := clientA.PrepareImage(ctx, prepReq)
	if err != nil {
		t.Fatalf("Node A PrepareImage failed: %v", err)
	}
	if resA.ProviderName != ProviderNydusDragonfly {
		t.Errorf("Node A ProviderName = %q, want %q", resA.ProviderName, ProviderNydusDragonfly)
	}
	if upstreamHits.Load() != 1 || dfNodeA.upstreamMisses != 1 {
		t.Fatalf("after Node A cold pull: upstreamHits=%d dfNodeA.upstreamMisses=%d, want 1", upstreamHits.Load(), dfNodeA.upstreamMisses)
	}

	// Step 2: Cold PrepareImage + demand-paging on Node B -> fetches piece via cross-node P2P from Node A with 0 upstream GAR hits.
	resB, err := clientB.PrepareImage(ctx, prepReq)
	if err != nil {
		t.Fatalf("Node B PrepareImage failed: %v", err)
	}
	gotPayload, err := os.ReadFile(filepath.Join(resB.LayerDirs[0], "fs", "chunk.bin"))
	if err != nil {
		t.Fatalf("reading Node B EROFS chunk.bin: %v", err)
	}
	if string(gotPayload) != chunkPayload {
		t.Errorf("Node B chunk.bin = %q, want %q", string(gotPayload), chunkPayload)
	}
	if upstreamHits.Load() != 1 {
		t.Errorf("upstreamHits after Node B P2P pull = %d, want 1 (0 new upstream requests from Node B)", upstreamHits.Load())
	}
	if dfNodeB.peerHits != 1 || dfNodeB.upstreamMisses != 0 {
		t.Errorf("Node B dfdaemon stats: peerHits=%d upstreamMisses=%d, want peerHits=1 upstreamMisses=0", dfNodeB.peerHits, dfNodeB.upstreamMisses)
	}
}

func TestNydusDragonflyUnconvertedOCIImageDeclinesWithErrNotSupported(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	snapClient := &fakeNydusSnapshotsClient{
		committed: make(map[string]bool),
		activeDir: tmpDir,
	}

	driver, err := NewNydusDragonfly(NydusDragonflyConfig{
		SkipKernelCheck:     true,
		VerifyDragonflyPing: false,
		LayerAnnotations: func(_ context.Context, _ string, _ *ocilayer.AuthConfig) ([]map[string]string, error) {
			return []map[string]string{{}}, nil // standard OCI layer with no nydus-bootstrap annotation
		},
		AdapterOpts: []adapter.Option{
			adapter.WithWorkDir(filepath.Join(tmpDir, "work")),
			adapter.WithSnapshotsClient(snapClient),
			adapter.WithImageResolver(func(_ context.Context, _ string, _ *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error) {
				return "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
					&v1.Config{},
					[]string{"sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"},
					[]string{"sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
					nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("NewNydusDragonfly: %v", err)
	}

	client := startTestNydusDragonflyServer(t, driver)
	_, err = client.PrepareImage(ctx, &ocilayer.PrepareRequest{
		ImageRef: "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/image-a:latest",
	})
	if !errors.Is(err, ocilayer.ErrNotSupported) {
		t.Fatalf("PrepareImage error = %v, want ErrNotSupported for non-Nydus OCI image", err)
	}
}

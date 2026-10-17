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
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func startTestNydusServer(t *testing.T, driver *NydusDriver) *ocilayer.GRPCClient {
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

func TestResolveDragonflyProxyAddr(t *testing.T) {
	t.Setenv(EnvDragonflyProxyAddr, "")
	addr, err := ResolveDragonflyProxyAddr("")
	if err != nil || addr != DefaultDragonflyProxyAddr {
		t.Fatalf("default ResolveDragonflyProxyAddr = (%q, %v), want (%q, nil)", addr, err, DefaultDragonflyProxyAddr)
	}

	t.Setenv(EnvDragonflyProxyAddr, "http://10.96.0.10:65001")
	addr, err = ResolveDragonflyProxyAddr("")
	if err != nil || addr != "http://10.96.0.10:65001" {
		t.Fatalf("env ResolveDragonflyProxyAddr = (%q, %v), want (http://10.96.0.10:65001, nil)", addr, err)
	}

	if _, err := ResolveDragonflyProxyAddr("not-a-url"); err == nil {
		t.Fatal("expected error for invalid Dragonfly proxy URL")
	}
}

func TestNydusEROFSBootstrapAndRejectFUSEFallback(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	bootstrapActiveDir := filepath.Join(tmpDir, "nydus-bootstrap-active")
	if err := os.MkdirAll(bootstrapActiveDir, 0o755); err != nil {
		t.Fatal(err)
	}

	blobDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	bootDigest := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	diffIDs := []string{
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
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

	bootstrapTar := makeTestTar(t, map[string]string{
		"image/image.boot": "rafs-v6-erofs-superblock",
	})

	var mountedOpts string
	driver, err := NewNydus(NydusConfig{
		SkipKernelCheck:    true,
		DockerConfigPath:   filepath.Join(tmpDir, "docker", "config.json"),
		DragonflyProxyAddr: "http://127.0.0.1:65001",
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
		LayerReader: func(_ context.Context, _ string, _ *ocilayer.AuthConfig, layerIndex int, layerDigest string) (io.ReadCloser, error) {
			if layerIndex != 1 || layerDigest != bootDigest {
				t.Errorf("unexpected LayerReader call: index=%d digest=%s", layerIndex, layerDigest)
			}
			return io.NopCloser(bytes.NewReader(bootstrapTar)), nil
		},
		MountFS: func(_ string, targetDir, fstype string, _ uintptr, data string) error {
			if fstype != "erofs" {
				t.Errorf("fstype = %q, want erofs", fstype)
			}
			mountedOpts = data
			return os.WriteFile(filepath.Join(targetDir, "etc-os-release"), []byte("EROFS-OK"), 0o644)
		},
		UnmountFS: func(_ string, _ int) error {
			return nil
		},
		StatFS: func(_ string) (int64, error) {
			return EROFSSuperMagic, nil
		},
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
		t.Fatalf("NewNydus: %v", err)
	}

	client := startTestNydusServer(t, driver)
	req := &ocilayer.PrepareRequest{
		ImageRef: "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-images/sandbox:nydus-v6",
		AuthConfig: &ocilayer.AuthConfig{
			Username: "oauth2accesstoken",
			Password: "token-value",
		},
	}

	res, err := client.PrepareImage(ctx, req)
	if err != nil {
		t.Fatalf("PrepareImage failed: %v", err)
	}
	if len(res.LayerDirs) != 1 {
		t.Fatalf("len(LayerDirs) = %d, want 1 merged EROFS view", len(res.LayerDirs))
	}
	if mountedOpts != "fsid=nydus-fsid-1,domain_id=nydus-domain-1" {
		t.Errorf("mountedOpts = %q, want fsid=nydus-fsid-1,domain_id=nydus-domain-1", mountedOpts)
	}
	if _, err := os.Stat(filepath.Join(res.LayerDirs[0], "fs", "etc-os-release")); err != nil {
		t.Errorf("expected EROFS mounted file at layer-0/fs/etc-os-release: %v", err)
	}
	if _, err := os.Stat(filepath.Join(bootstrapActiveDir, "image", "image.boot")); err != nil {
		t.Errorf("expected unpacked image/image.boot in active snapshot dir: %v", err)
	}
}

func TestNydusUnconvertedOCIImageDeclinesWithErrNotSupported(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	snapClient := &fakeNydusSnapshotsClient{
		committed: make(map[string]bool),
		activeDir: tmpDir,
	}

	driver, err := NewNydus(NydusConfig{
		SkipKernelCheck: true,
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
		t.Fatalf("NewNydus: %v", err)
	}

	client := startTestNydusServer(t, driver)
	_, err = client.PrepareImage(ctx, &ocilayer.PrepareRequest{
		ImageRef: "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/image-a:latest",
	})
	if !errors.Is(err, ocilayer.ErrNotSupported) {
		t.Fatalf("PrepareImage error = %v, want ErrNotSupported for non-Nydus OCI image", err)
	}
}

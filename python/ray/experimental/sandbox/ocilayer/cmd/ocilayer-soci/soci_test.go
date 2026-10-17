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
	"sync"
	"testing"
	"time"

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

type fakeSOCISnapshotsClient struct {
	mu               sync.Mutex
	mountDirs        map[string]string
	committed        map[string]bool
	defaultActiveDir string
	remoteLayers     map[string]bool
	activeDirs       map[string]string
	prepareReqs      []*snapshots.PrepareSnapshotRequest
	commitReqs       []*snapshots.CommitSnapshotRequest
}

func (f *fakeSOCISnapshotsClient) Prepare(_ context.Context, in *snapshots.PrepareSnapshotRequest, _ ...grpc.CallOption) (*snapshots.PrepareSnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepareReqs = append(f.prepareReqs, in)

	targetChainID := in.GetLabels()["containerd.io/snapshot.ref"]
	layerDigest := in.GetLabels()["containerd.io/snapshot/cri.layer-digest"]
	if f.remoteLayers[layerDigest] {
		if targetChainID != "" {
			f.committed[targetChainID] = true
		}
		return nil, status.Error(codes.AlreadyExists, "remote layer already prepared by soci")
	}

	activeDir, ok := f.activeDirs[in.GetKey()]
	if !ok || activeDir == "" {
		activeDir = f.defaultActiveDir
	}
	f.activeDirs[in.GetKey()] = activeDir
	return &snapshots.PrepareSnapshotResponse{
		Mounts: []*snapshots.Mount{
			{
				Type:    "overlay",
				Source:  "overlay",
				Options: []string{"upperdir=" + activeDir},
			},
		},
	}, nil
}

func (f *fakeSOCISnapshotsClient) View(_ context.Context, in *snapshots.ViewSnapshotRequest, _ ...grpc.CallOption) (*snapshots.ViewSnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &snapshots.ViewSnapshotResponse{
		Mounts: []*snapshots.Mount{
			{
				Type:    "bind",
				Source:  f.mountDirs[in.GetParent()],
				Options: []string{"ro", "rbind"},
			},
		},
	}, nil
}

func (f *fakeSOCISnapshotsClient) Mounts(_ context.Context, _ *snapshots.MountsRequest, _ ...grpc.CallOption) (*snapshots.MountsResponse, error) {
	return &snapshots.MountsResponse{}, nil
}

func (f *fakeSOCISnapshotsClient) Commit(_ context.Context, in *snapshots.CommitSnapshotRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitReqs = append(f.commitReqs, in)
	f.committed[in.GetName()] = true
	if dir, ok := f.activeDirs[in.GetKey()]; ok {
		f.mountDirs[in.GetName()] = dir
	}
	return &emptypb.Empty{}, nil
}

func (f *fakeSOCISnapshotsClient) Remove(_ context.Context, _ *snapshots.RemoveSnapshotRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeSOCISnapshotsClient) Stat(_ context.Context, in *snapshots.StatSnapshotRequest, _ ...grpc.CallOption) (*snapshots.StatSnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.committed[in.GetKey()] {
		return &snapshots.StatSnapshotResponse{}, nil
	}
	return nil, status.Error(codes.NotFound, "not found")
}

func startTestSOCIServer(t *testing.T, driver *SOCIDriver) *ocilayer.GRPCClient {
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

func TestSOCISmallLayerMaterializationAndAllOrNothing(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	remoteMountDir := filepath.Join(tmpDir, "soci-fuse-layer0")
	if err := os.MkdirAll(remoteMountDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(remoteMountDir, "model.bin"), []byte("streamed-weights"), 0o644); err != nil {
		t.Fatal(err)
	}
	smallLayerMountDir := filepath.Join(tmpDir, "soci-local-layer1")
	if err := os.MkdirAll(smallLayerMountDir, 0o755); err != nil {
		t.Fatal(err)
	}

	largeDigest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	smallDigest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	diffIDs := []string{
		"sha256:1111111111111111111111111111111111111111111111111111111111111111",
		"sha256:2222222222222222222222222222222222222222222222222222222222222222",
	}

	snapClient := &fakeSOCISnapshotsClient{
		mountDirs: map[string]string{
			diffIDs[0]: remoteMountDir,
		},
		committed:        make(map[string]bool),
		defaultActiveDir: smallLayerMountDir,
		remoteLayers: map[string]bool{
			largeDigest: true,
			smallDigest: false, // skipped by soci create (< min-layer-size)
		},
		activeDirs: make(map[string]string),
	}

	smallTarBytes := makeTestTar(t, map[string]string{
		"etc/agent.conf": "mode=production\n",
	})

	dockerCfgPath := filepath.Join(tmpDir, "docker", "config.json")
	driver, err := NewSOCI(SOCIConfig{
		DockerConfigPath: dockerCfgPath,
		LayerReader: func(_ context.Context, _ string, _ *ocilayer.AuthConfig, layerIndex int, layerDigest string) (io.ReadCloser, error) {
			if layerIndex != 1 || layerDigest != smallDigest {
				t.Errorf("unexpected layerReader call: index=%d digest=%s", layerIndex, layerDigest)
			}
			return io.NopCloser(bytes.NewReader(smallTarBytes)), nil
		},
		AdapterOpts: []adapter.Option{
			adapter.WithWorkDir(filepath.Join(tmpDir, "work")),
			adapter.WithSnapshotsClient(snapClient),
			adapter.WithLeaseIdleTTL(0),
			adapter.WithImageResolver(func(_ context.Context, _ string, _ *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error) {
				return "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
					&v1.Config{Entrypoint: []string{"/bin/soci-agent"}},
					diffIDs,
					[]string{largeDigest, smallDigest},
					nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("NewSOCI: %v", err)
	}

	client := startTestSOCIServer(t, driver)
	req := &ocilayer.PrepareRequest{
		ImageRef: "123456789012.dkr.ecr.us-west-2.amazonaws.com/agent:v1",
		AuthConfig: &ocilayer.AuthConfig{
			Username: "AWS",
			Password: "ecr-token-value",
		},
	}
	if ok, err := client.CanProvide(ctx, req); err != nil || !ok {
		t.Fatalf("CanProvide = (%v, %v), want (true, nil)", ok, err)
	}
	if client.Name() != ProviderSOCI {
		t.Fatalf("client.Name() = %q, want %q", client.Name(), ProviderSOCI)
	}

	res, err := client.PrepareImage(ctx, req)
	if err != nil {
		t.Fatalf("PrepareImage failed: %v", err)
	}
	if len(res.LayerDirs) != 2 {
		t.Fatalf("len(LayerDirs) = %d, want 2", len(res.LayerDirs))
	}

	if _, err := os.Stat(dockerCfgPath); err != nil {
		t.Fatalf("expected docker config file at %s: %v", dockerCfgPath, err)
	}

	if _, err := os.Stat(filepath.Join(res.LayerDirs[0], "fs", "model.bin")); err != nil {
		t.Errorf("layer 0 missing remote streamed file: %v", err)
	}
	gotConf, err := os.ReadFile(filepath.Join(res.LayerDirs[1], "fs", "etc", "agent.conf"))
	if err != nil {
		t.Fatalf("layer 1 missing locally materialized file: %v", err)
	}
	if string(gotConf) != "mode=production\n" {
		t.Errorf("layer 1 etc/agent.conf = %q, want %q", string(gotConf), "mode=production\n")
	}

	snapClient.mu.Lock()
	if len(snapClient.commitReqs) != 1 {
		t.Fatalf("expected 1 Commit RPC for the small layer, got %d", len(snapClient.commitReqs))
	}
	snapClient.mu.Unlock()
}

func TestSOCIUnindexedImageDeclinesAllOrNothing(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	localMountDir := filepath.Join(tmpDir, "soci-unindexed")
	if err := os.MkdirAll(localMountDir, 0o755); err != nil {
		t.Fatal(err)
	}

	layerDigest := "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	diffIDs := []string{"sha256:3333333333333333333333333333333333333333333333333333333333333333"}

	snapClient := &fakeSOCISnapshotsClient{
		mountDirs:        make(map[string]string),
		committed:        make(map[string]bool),
		defaultActiveDir: localMountDir,
		remoteLayers:     map[string]bool{},
		activeDirs:       make(map[string]string),
	}

	smallTarBytes := makeTestTar(t, map[string]string{"hello.txt": "world"})
	driver, err := NewSOCI(SOCIConfig{
		LayerReader: func(_ context.Context, _ string, _ *ocilayer.AuthConfig, _ int, _ string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(smallTarBytes)), nil
		},
		AdapterOpts: []adapter.Option{
			adapter.WithWorkDir(filepath.Join(tmpDir, "work")),
			adapter.WithSnapshotsClient(snapClient),
			adapter.WithDeclineTTL(5 * time.Minute),
			adapter.WithImageResolver(func(_ context.Context, _ string, _ *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error) {
				return "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
					&v1.Config{},
					diffIDs,
					[]string{layerDigest},
					nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("NewSOCI: %v", err)
	}

	client := startTestSOCIServer(t, driver)
	_, err = client.PrepareImage(ctx, &ocilayer.PrepareRequest{
		ImageRef: "123456789012.dkr.ecr.us-west-2.amazonaws.com/unindexed:v1",
	})
	if !errors.Is(err, ocilayer.ErrNotSupported) {
		t.Fatalf("PrepareImage error = %v, want ErrNotSupported when 0 layers are remote-streamed", err)
	}
}

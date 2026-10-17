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
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ocilayer"
	ocilayerpb "github.com/agent-substrate/substrate/internal/proto/ocilayer"
	"github.com/agent-substrate/substrate/internal/proto/snapshots"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeSnapshotsClient struct {
	mu          sync.Mutex
	mountDirs   map[string]string
	committed   map[string]bool
	declinePrep bool
	prepareReqs []*snapshots.PrepareSnapshotRequest
	viewReqs    []*snapshots.ViewSnapshotRequest
	removeKeys  []string
}

func newFakeSnapshotsClient(t *testing.T, diffIDs []string) *fakeSnapshotsClient {
	t.Helper()
	f := &fakeSnapshotsClient{
		mountDirs: make(map[string]string),
		committed: make(map[string]bool),
	}
	for _, c := range ComputeChainIDs(diffIDs) {
		dir := filepath.Join(t.TempDir(), SanitizePathKey(c.ChainID))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("streamed"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		f.mountDirs[c.ChainID] = dir
	}
	return f
}

func (f *fakeSnapshotsClient) Prepare(_ context.Context, in *snapshots.PrepareSnapshotRequest, _ ...grpc.CallOption) (*snapshots.PrepareSnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepareReqs = append(f.prepareReqs, in)
	if f.declinePrep {
		return &snapshots.PrepareSnapshotResponse{}, nil
	}
	chainID := in.GetLabels()["containerd.io/snapshot.ref"]
	if chainID != "" {
		f.committed[chainID] = true
	}
	return nil, status.Error(codes.AlreadyExists, "already exists")
}

func (f *fakeSnapshotsClient) View(_ context.Context, in *snapshots.ViewSnapshotRequest, _ ...grpc.CallOption) (*snapshots.ViewSnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.viewReqs = append(f.viewReqs, in)
	dir := f.mountDirs[in.GetParent()]
	return &snapshots.ViewSnapshotResponse{
		Mounts: []*snapshots.Mount{{
			Type:   "bind",
			Source: dir,
		}},
	}, nil
}

func (f *fakeSnapshotsClient) Mounts(_ context.Context, _ *snapshots.MountsRequest, _ ...grpc.CallOption) (*snapshots.MountsResponse, error) {
	return &snapshots.MountsResponse{}, nil
}

func (f *fakeSnapshotsClient) Commit(_ context.Context, _ *snapshots.CommitSnapshotRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeSnapshotsClient) Remove(_ context.Context, in *snapshots.RemoveSnapshotRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removeKeys = append(f.removeKeys, in.GetKey())
	return &emptypb.Empty{}, nil
}

func (f *fakeSnapshotsClient) Stat(_ context.Context, in *snapshots.StatSnapshotRequest, _ ...grpc.CallOption) (*snapshots.StatSnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.committed[in.GetKey()] {
		return &snapshots.StatSnapshotResponse{}, nil
	}
	return nil, status.Error(codes.NotFound, "not found")
}

func startTestOCILayerServer(t *testing.T, driver *Driver) *ocilayer.GRPCClient {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "ocilayer.sock")
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := grpc.NewServer()
	ocilayerpb.RegisterOCILayerServiceServer(srv, NewServer(driver))
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

func TestServerAndDriverLifecycle(t *testing.T) {
	ctx := context.Background()
	diffIDs := []string{
		"sha256:1111111111111111111111111111111111111111111111111111111111111111",
		"sha256:2222222222222222222222222222222222222222222222222222222222222222",
	}
	layerDigests := []string{
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	imgDigest := "sha256:9999999999999999999999999999999999999999999999999999999999999999"

	snapClient := newFakeSnapshotsClient(t, diffIDs)
	workDir := filepath.Join(t.TempDir(), "work")

	driver, err := NewDriver("test-provider", "/run/test.sock", "test-snap",
		WithWorkDir(workDir),
		WithSnapshotsClient(snapClient),
		WithLeaseIdleTTL(10*time.Minute),
		WithImageResolver(func(_ context.Context, _ string, _ *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error) {
			return imgDigest, &v1.Config{Entrypoint: []string{"/bin/agent"}}, diffIDs, layerDigests, nil
		}),
	)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}

	client := startTestOCILayerServer(t, driver)
	imageRef := "us-central1-docker.pkg.dev/my-proj/repo/agent:v1"
	req := &ocilayer.PrepareRequest{ImageRef: imageRef}

	ok, err := client.CanProvide(ctx, req)
	if err != nil || !ok {
		t.Fatalf("CanProvide = (%v, %v), want (true, nil)", ok, err)
	}
	if client.Name() != "test-provider" {
		t.Errorf("client.Name() = %q, want %q", client.Name(), "test-provider")
	}

	// 1. First PrepareImage mounts the 2 layers.
	res1, err := client.PrepareImage(ctx, req)
	if err != nil {
		t.Fatalf("PrepareImage(1) failed: %v", err)
	}
	if len(res1.LayerDirs) != 2 {
		t.Fatalf("len(LayerDirs) = %d, want 2", len(res1.LayerDirs))
	}
	if res1.RootfsDir == "" {
		t.Fatalf("res1.RootfsDir is empty")
	}
	if _, err := os.Stat(filepath.Join(res1.RootfsDir, "hello.txt")); err != nil {
		t.Errorf("rootfsDir %q missing streamed file: %v", res1.RootfsDir, err)
	}
	for i, dir := range res1.LayerDirs {
		if _, err := os.Stat(filepath.Join(dir, "fs", "hello.txt")); err != nil {
			t.Errorf("layer %d missing streamed file: %v", i, err)
		}
		if _, err := os.Stat(filepath.Join(dir, "finalized")); err != nil {
			t.Errorf("layer %d missing finalized marker: %v", i, err)
		}
	}

	// 2. Second PrepareImage reuses warm lease and increments refCount to 2 without new View RPCs.
	res2, err := client.PrepareImage(ctx, req)
	if err != nil {
		t.Fatalf("PrepareImage(2) failed: %v", err)
	}
	snapClient.mu.Lock()
	if len(snapClient.viewReqs) != 2 {
		t.Errorf("expected 2 View RPCs total after warm hit, got %d", len(snapClient.viewReqs))
	}
	snapClient.mu.Unlock()
	if len(res2.LayerDirs) != 2 {
		t.Fatalf("len(res2.LayerDirs) = %d, want 2", len(res2.LayerDirs))
	}
	if res2.RootfsDir != res1.RootfsDir {
		t.Errorf("res2.RootfsDir = %q, want %q", res2.RootfsDir, res1.RootfsDir)
	}

	// 3. First ReleaseImage decrements refCount from 2 -> 1 (views stay mounted).
	if err := client.ReleaseImage(ctx, &ocilayer.ReleaseRequest{ImageRef: imageRef, BypassTTL: true}); err != nil {
		t.Fatalf("ReleaseImage(1) failed: %v", err)
	}
	snapClient.mu.Lock()
	if len(snapClient.removeKeys) != 0 {
		t.Errorf("expected 0 Remove calls while refCount=1, got %d", len(snapClient.removeKeys))
	}
	snapClient.mu.Unlock()

	// 4. Second ReleaseImage with BypassTTL=true decrements refCount 1 -> 0 and immediately removes views.
	if err := client.ReleaseImage(ctx, &ocilayer.ReleaseRequest{ImageRef: imageRef, BypassTTL: true}); err != nil {
		t.Fatalf("ReleaseImage(2) failed: %v", err)
	}
	snapClient.mu.Lock()
	if len(snapClient.removeKeys) != 2 {
		t.Errorf("expected 2 Remove calls after refCount reached 0 with BypassTTL, got %d", len(snapClient.removeKeys))
	}
	snapClient.mu.Unlock()
}

func TestServerDeclineReturnsErrNotSupported(t *testing.T) {
	ctx := context.Background()
	diffIDs := []string{"sha256:1111111111111111111111111111111111111111111111111111111111111111"}
	snapClient := newFakeSnapshotsClient(t, diffIDs)
	snapClient.declinePrep = true

	driver, err := NewDriver("test-provider", "/run/test.sock", "test-snap",
		WithWorkDir(filepath.Join(t.TempDir(), "work")),
		WithSnapshotsClient(snapClient),
		WithImageResolver(func(_ context.Context, _ string, _ *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error) {
			return "sha256:1234", &v1.Config{}, diffIDs, diffIDs, nil
		}),
	)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}

	client := startTestOCILayerServer(t, driver)
	_, err = client.PrepareImage(ctx, &ocilayer.PrepareRequest{ImageRef: "docker.io/library/alpine:latest"})
	if !errors.Is(err, ocilayer.ErrNotSupported) {
		t.Fatalf("PrepareImage error = %v, want ErrNotSupported", err)
	}
}

func TestServerReconcileImagesRestoresAndSweepsOrphans(t *testing.T) {
	ctx := context.Background()
	diffIDs := []string{"sha256:1111111111111111111111111111111111111111111111111111111111111111"}
	snapClient := newFakeSnapshotsClient(t, diffIDs)
	workDir := filepath.Join(t.TempDir(), "work")

	driver, err := NewDriver("test-provider", "/run/test.sock", "test-snap",
		WithWorkDir(workDir),
		WithSnapshotsClient(snapClient),
		WithLeaseIdleTTL(0),
		WithImageResolver(func(_ context.Context, _ string, _ *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error) {
			return "sha256:1234", &v1.Config{}, diffIDs, diffIDs, nil
		}),
	)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}

	client := startTestOCILayerServer(t, driver)
	activeImg := "example.com/active:v1"
	orphanImg := "example.com/orphan:v1"

	activeRes, err := client.PrepareImage(ctx, &ocilayer.PrepareRequest{ImageRef: activeImg})
	if err != nil {
		t.Fatalf("PrepareImage(active): %v", err)
	}
	orphanRes, err := client.PrepareImage(ctx, &ocilayer.PrepareRequest{ImageRef: orphanImg})
	if err != nil {
		t.Fatalf("PrepareImage(orphan): %v", err)
	}

	// Reconcile only activeImg with RefCount=3; orphanImg should be swept immediately.
	if err := client.ReconcileImages(ctx, []*ocilayer.ActiveImageLayers{
		{
			ImageRef:    activeImg,
			ImageDigest: activeRes.ImageDigest,
			LayerDirs:   activeRes.LayerDirs,
			RefCount:    3,
		},
	}); err != nil {
		t.Fatalf("ReconcileImages: %v", err)
	}

	if _, err := os.Stat(orphanRes.LayerDirs[0]); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphan layer dir still exists after ReconcileImages: err=%v", err)
	}
	if _, err := os.Stat(activeRes.LayerDirs[0]); err != nil {
		t.Errorf("active layer dir missing after ReconcileImages: %v", err)
	}
}

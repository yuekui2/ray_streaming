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
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ocilayer"
	"github.com/agent-substrate/substrate/internal/ocilayer/adapter"
	ocilayerpb "github.com/agent-substrate/substrate/internal/proto/ocilayer"
	"github.com/agent-substrate/substrate/internal/proto/riptidekeychain"
	"github.com/agent-substrate/substrate/internal/proto/snapshots"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeSnapshotsClient struct {
	mu        sync.Mutex
	mountDirs map[string]string
	committed map[string]bool
	viewReqs  []*snapshots.ViewSnapshotRequest
}

func newFakeSnapshotsClient(t *testing.T, diffIDs []string) *fakeSnapshotsClient {
	t.Helper()
	f := &fakeSnapshotsClient{
		mountDirs: make(map[string]string),
		committed: make(map[string]bool),
	}
	for _, c := range adapter.ComputeChainIDs(diffIDs) {
		dir := filepath.Join(t.TempDir(), adapter.SanitizePathKey(c.ChainID))
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
	return &snapshots.ViewSnapshotResponse{
		Mounts: []*snapshots.Mount{{
			Type:   "bind",
			Source: f.mountDirs[in.GetParent()],
		}},
	}, nil
}

func (f *fakeSnapshotsClient) Mounts(_ context.Context, _ *snapshots.MountsRequest, _ ...grpc.CallOption) (*snapshots.MountsResponse, error) {
	return &snapshots.MountsResponse{}, nil
}

func (f *fakeSnapshotsClient) Commit(_ context.Context, _ *snapshots.CommitSnapshotRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeSnapshotsClient) Remove(_ context.Context, _ *snapshots.RemoveSnapshotRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
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

type fakeRiptideKeychain struct {
	mu      sync.Mutex
	updates []*riptidekeychain.UpdateCredsRequest
}

func (k *fakeRiptideKeychain) UpdateCreds(_ context.Context, in *riptidekeychain.UpdateCredsRequest, _ ...grpc.CallOption) (*riptidekeychain.UpdateCredsResponse, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.updates = append(k.updates, in)
	return &riptidekeychain.UpdateCredsResponse{}, nil
}

func (k *fakeRiptideKeychain) RemoveCreds(_ context.Context, _ *riptidekeychain.RemoveCredsRequest, _ ...grpc.CallOption) (*riptidekeychain.RemoveCredsResponse, error) {
	return &riptidekeychain.RemoveCredsResponse{}, nil
}

func TestRiptideKeychainPushAndDeduplication(t *testing.T) {
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
	kcClient := &fakeRiptideKeychain{}

	driver, err := NewRiptide(RiptideConfig{
		KeychainClient: kcClient,
		AdapterOpts: []adapter.Option{
			adapter.WithWorkDir(filepath.Join(t.TempDir(), "work")),
			adapter.WithSnapshotsClient(snapClient),
			adapter.WithLeaseIdleTTL(10 * time.Minute),
			adapter.WithImageResolver(func(_ context.Context, _ string, _ *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error) {
				return imgDigest, &v1.Config{Entrypoint: []string{"/bin/agent"}}, diffIDs, layerDigests, nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("NewRiptide: %v", err)
	}

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

	imageRef := "us-central1-docker.pkg.dev/my-proj/repo/agent:v1"
	req := &ocilayer.PrepareRequest{
		ImageRef: imageRef,
		AuthConfig: &ocilayer.AuthConfig{
			Username: "oauth2accesstoken",
			Password: "test-access-token",
		},
	}

	ok, err := client.CanProvide(ctx, req)
	if err != nil || !ok {
		t.Fatalf("CanProvide = (%v, %v), want (true, nil)", ok, err)
	}
	if client.Name() != ProviderRiptide {
		t.Errorf("client.Name() = %q, want %q", client.Name(), ProviderRiptide)
	}

	// First PrepareImage pushes credentials to gcfsd keychain.sock.
	res1, err := client.PrepareImage(ctx, req)
	if err != nil {
		t.Fatalf("PrepareImage(1) failed: %v", err)
	}
	if len(res1.LayerDirs) != 2 {
		t.Fatalf("len(LayerDirs) = %d, want 2", len(res1.LayerDirs))
	}

	kcClient.mu.Lock()
	if len(kcClient.updates) != 1 {
		t.Fatalf("expected 1 Keychain.UpdateCreds call, got %d", len(kcClient.updates))
	}
	wantCanonical := "us-central1-docker.pkg.dev/my-proj/repo/agent@" + imgDigest
	if got := kcClient.updates[0].GetImage(); got != wantCanonical {
		t.Errorf("UpdateCreds image = %q, want %q", got, wantCanonical)
	}
	kcClient.mu.Unlock()

	// Warm hit with identical credentials skips redundant UpdateCreds RPC within TTL.
	if _, err := client.PrepareImage(ctx, req); err != nil {
		t.Fatalf("PrepareImage(2) failed: %v", err)
	}
	kcClient.mu.Lock()
	if len(kcClient.updates) != 1 {
		t.Errorf("expected 1 Keychain.UpdateCreds call after warm hit with identical creds, got %d", len(kcClient.updates))
	}
	kcClient.mu.Unlock()
}

func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

// TestBuildGCFSDAndSnapshotterCommands mirrors test_gcfs_usernamespace_support
// in gke-internal-configure-helper-test.sh and verifies all flags from
// gke-setup-gcfs in gke-internal-configure-helper.sh.
func TestBuildGCFSDAndSnapshotterCommands(t *testing.T) {
	// Case 1: V2=false, userns=false -> gcfsd binary, no --enable_id_mapping, no --enable-v2
	bin1, args1, env1 := BuildGCFSDCommand(RiptideDaemonConfig{
		EnableV2API:                false,
		EnableUserNamespaceSupport: false,
	})
	if bin1 != "/home/kubernetes/bin/gcfsd" {
		t.Errorf("Case 1 bin = %q, want /home/kubernetes/bin/gcfsd", bin1)
	}
	if containsArg(args1, "--enable_id_mapping") {
		t.Errorf("Case 1: expected no --enable_id_mapping when V2=false, userns=false")
	}
	if !containsArg(env1, "GOGC=10") {
		t.Errorf("Case 1: expected GOGC=10 in env, got %v", env1)
	}
	_, snapArgs1, _ := BuildGCFSSnapshotterCommand(RiptideDaemonConfig{
		EnableV2API: false,
	})
	if containsArg(snapArgs1, "--enable-v2") {
		t.Errorf("Case 1: expected no --enable-v2 when V2=false")
	}

	// Case 2: V2=false, userns=true -> V1 gcfsd does not support --enable_id_mapping
	bin2, args2, _ := BuildGCFSDCommand(RiptideDaemonConfig{
		EnableV2API:                false,
		EnableUserNamespaceSupport: true,
	})
	if bin2 != "/home/kubernetes/bin/gcfsd" {
		t.Errorf("Case 2 bin = %q, want /home/kubernetes/bin/gcfsd", bin2)
	}
	if containsArg(args2, "--enable_id_mapping") {
		t.Errorf("Case 2: expected no --enable_id_mapping on V1 gcfsd when V2=false, userns=true")
	}

	// Case 3: V2=true, userns=false -> gcfsd-v2 used, no --enable_id_mapping, snapshotter has --enable-v2
	bin3, args3, _ := BuildGCFSDCommand(RiptideDaemonConfig{
		EnableV2API:                true,
		EnableUserNamespaceSupport: false,
		EnableLayerCaching:         true,
		CacheSizeMiB:               2048,
	})
	if bin3 != "/home/kubernetes/bin/gcfsd-v2" {
		t.Errorf("Case 3 bin = %q, want /home/kubernetes/bin/gcfsd-v2", bin3)
	}
	if containsArg(args3, "--enable_id_mapping") {
		t.Errorf("Case 3: expected no --enable_id_mapping when V2=true, userns=false")
	}
	for _, want := range []string{
		"--mount_point=/run/gcfsd/mnt",
		"--max_content_cache_size_mb=1024",
		"--max_large_files_cache_size_mb=1024",
		"--layer_cache_dir=/var/lib/substrate/gcfs/snapshotter/layers",
		"--images_in_use_db_path=/var/lib/substrate/gcfs/gcfsd/images_in_use.db",
		"--enable_pull_secret_keychain",
		"--client_name=GKE",
		"--enable_single_flighting=true",
		"--read_ahead_max_blocks=10",
		"--read_ahead_cache_size_ratio=0.1",
	} {
		if !containsArg(args3, want) {
			t.Errorf("Case 3 missing expected gcfsd-v2 flag %q in %v", want, args3)
		}
	}

	snapBin3, snapArgs3, snapEnv3 := BuildGCFSSnapshotterCommand(RiptideDaemonConfig{
		EnableV2API:                  true,
		EnableRiptideImagePreloading: true,
	})
	if snapBin3 != "/home/kubernetes/bin/containerd-gcfs-grpc" {
		t.Errorf("Case 3 snapshotter bin = %q, want /home/kubernetes/bin/containerd-gcfs-grpc", snapBin3)
	}
	for _, want := range []string{
		"--log-level=info",
		"--config=/etc/containerd-gcfs-grpc/config.toml",
		"--address=/run/ate-image-streaming/gcfs-snapshotter.sock",
		"--root=/var/lib/substrate/gcfs",
		"--gcfs-socket-path=/run/gcfsd/grpc.sock",
		"--enable-image-proxy-keychain-client",
		"--disable-duplicate-layer-support=false",
		"--enable-metric-exporter=false",
		"--enable-v2",
	} {
		if !containsArg(snapArgs3, want) {
			t.Errorf("Case 3 missing expected containerd-gcfs-grpc flag %q in %v", want, snapArgs3)
		}
	}
	if !containsArg(snapEnv3, "HOME=/root") {
		t.Errorf("Case 3 expected HOME=/root in snapshotter env, got %v", snapEnv3)
	}

	// Case 4: V2=true, userns=true -> gcfsd-v2 and --enable_id_mapping enabled
	bin4, args4, _ := BuildGCFSDCommand(RiptideDaemonConfig{
		EnableV2API:                true,
		EnableUserNamespaceSupport: true,
	})
	if bin4 != "/home/kubernetes/bin/gcfsd-v2" {
		t.Errorf("Case 4 bin = %q, want /home/kubernetes/bin/gcfsd-v2", bin4)
	}
	if !containsArg(args4, "--enable_id_mapping") {
		t.Errorf("Case 4: expected --enable_id_mapping when V2=true, userns=true")
	}

	// Case 5: Isolated Substrate Riptide v2 stack (non-interfering with host gcfsd/containerd)
	isolatedCfg := RiptideDaemonConfig{
		MountPoint:                   "/var/lib/substrate/gcfs/mnt",
		ControlServerSocketPath:      "/run/gcfsd/grpc.sock",
		DownloaderUDSPath:            "/run/gcfsd/downloader.sock",
		SentinelFilePath:             "/var/lib/substrate/gcfs/gcfsd_sentinel.txt",
		DisableContainerdEvents:      true,
		DisableKubeletEventReporting: true,
		Port:                         11254,
		EnableV2API:                  true,
		EnableLayerCaching:           true,
		EnableRiptideImagePreloading: true,
	}
	_, args5, _ := BuildGCFSDCommand(isolatedCfg)
	for _, want := range []string{
		"--mount_point=/var/lib/substrate/gcfs/mnt",
		"--control_server_socket_path=/run/gcfsd/grpc.sock",
		"--downloader_uds_path=/run/gcfsd/downloader.sock",
		"--sentinel_file_path=/var/lib/substrate/gcfs/gcfsd_sentinel.txt",
		"--containerd_uds_path=",
		"--event_reporter_type=none",
		"--port=11254",
	} {
		if !containsArg(args5, want) {
			t.Errorf("Case 5 missing expected isolated gcfsd-v2 flag %q in %v", want, args5)
		}
	}
	_, snapArgs5, _ := BuildGCFSSnapshotterCommand(isolatedCfg)
	if !containsArg(snapArgs5, "--enable-kubernetes-event-reporting=false") {
		t.Errorf("Case 5 missing --enable-kubernetes-event-reporting=false in %v", snapArgs5)
	}
}



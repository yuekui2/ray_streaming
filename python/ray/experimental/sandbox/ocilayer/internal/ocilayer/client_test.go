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

package ocilayer_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/ocilayer"
	ocilayerpb "github.com/agent-substrate/substrate/internal/proto/ocilayer"
	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeServer struct {
	ocilayerpb.UnimplementedOCILayerServiceServer

	canProvideResp *ocilayerpb.CanProvideResponse
	prepareResp    *ocilayerpb.PrepareImageResponse
	prepareErr     error

	lastAuth      *ocilayerpb.RegistryAuth
	lastRelease   *ocilayerpb.ReleaseImageRequest
	lastReconcile *ocilayerpb.ReconcileImagesRequest
}

func (s *fakeServer) CanProvide(_ context.Context, req *ocilayerpb.CanProvideRequest) (*ocilayerpb.CanProvideResponse, error) {
	s.lastAuth = req.GetAuth()
	if s.canProvideResp != nil {
		return s.canProvideResp, nil
	}
	return &ocilayerpb.CanProvideResponse{CanProvide: true, ProviderName: "riptide"}, nil
}

func (s *fakeServer) PrepareImage(_ context.Context, req *ocilayerpb.PrepareImageRequest) (*ocilayerpb.PrepareImageResponse, error) {
	s.lastAuth = req.GetAuth()
	if s.prepareErr != nil {
		return nil, s.prepareErr
	}
	return s.prepareResp, nil
}

func (s *fakeServer) ReleaseImage(_ context.Context, req *ocilayerpb.ReleaseImageRequest) (*ocilayerpb.ReleaseImageResponse, error) {
	s.lastRelease = req
	return &ocilayerpb.ReleaseImageResponse{}, nil
}

func (s *fakeServer) ReconcileImages(_ context.Context, req *ocilayerpb.ReconcileImagesRequest) (*ocilayerpb.ReconcileImagesResponse, error) {
	s.lastReconcile = req
	return &ocilayerpb.ReconcileImagesResponse{}, nil
}

type staticKeychain struct {
	username string
	password string
}

func (k staticKeychain) Resolve(_ authn.Resource) (authn.Authenticator, error) {
	return authn.FromConfig(authn.AuthConfig{
		Username: k.username,
		Password: k.password,
	}), nil
}

func startFakeServer(t *testing.T, srv *fakeServer) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "ocilayer.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix %s: %v", sock, err)
	}
	grpcSrv := grpc.NewServer()
	ocilayerpb.RegisterOCILayerServiceServer(grpcSrv, srv)
	go func() {
		_ = grpcSrv.Serve(lis)
	}()
	t.Cleanup(func() {
		grpcSrv.Stop()
		_ = lis.Close()
	})
	return sock
}

func TestGRPCClientLifecycle(t *testing.T) {
	cfgJSON, err := json.Marshal(v1.Config{
		User:       "1000:1000",
		WorkingDir: "/workspace",
		Cmd:        []string{"/bin/agent"},
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}

	srv := &fakeServer{
		prepareResp: &ocilayerpb.PrepareImageResponse{
			ImageDigest:  "sha256:1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff",
			ConfigJson:   cfgJSON,
			LayerDirs:    []string{"/var/lib/atelet/streaming/riptide/img/layer-0/fs", "/var/lib/atelet/streaming/riptide/img/layer-1/fs"},
			ProviderName: "riptide",
		},
	}
	sock := startFakeServer(t, srv)

	client, err := ocilayer.NewGRPCClient(sock)
	if err != nil {
		t.Fatalf("NewGRPCClient: %v", err)
	}
	defer client.Close()

	ctx := ocilayer.WithKeychainContext(context.Background(), staticKeychain{
		username: "oauth2accesstoken",
		password: "secret-gar-token",
	})

	ok, err := client.CanProvide(ctx, &ocilayer.PrepareRequest{
		ImageRef: "us-central1-docker.pkg.dev/proj/repo/agent:v1",
	})
	if err != nil || !ok {
		t.Fatalf("CanProvide = (%v, %v), want (true, nil)", ok, err)
	}
	if got := client.Name(); got != "riptide" {
		t.Errorf("client.Name() = %q, want riptide", got)
	}

	res, err := client.PrepareImage(ctx, &ocilayer.PrepareRequest{
		ImageRef: "us-central1-docker.pkg.dev/proj/repo/agent:v1",
	})
	if err != nil {
		t.Fatalf("PrepareImage: %v", err)
	}
	if srv.lastAuth == nil || srv.lastAuth.GetPassword() != "secret-gar-token" {
		t.Fatalf("expected keychain credentials forwarded to adapter, got %+v", srv.lastAuth)
	}
	if len(res.LayerDirs) != 2 || res.Config == nil || res.Config.WorkingDir != "/workspace" {
		t.Fatalf("unexpected PrepareResult: %+v", res)
	}

	if err := client.ReleaseImage(ctx, &ocilayer.ReleaseRequest{
		ImageRef:  "us-central1-docker.pkg.dev/proj/repo/agent:v1",
		BypassTTL: true,
	}); err != nil {
		t.Fatalf("ReleaseImage: %v", err)
	}
	if srv.lastRelease == nil || !srv.lastRelease.GetBypassTtl() {
		t.Fatalf("expected BypassTtl=true in ReleaseImageRequest, got %+v", srv.lastRelease)
	}

	if err := client.ReconcileImages(ctx, []*ocilayer.ActiveImageLayers{
		{
			ImageRef:    "us-central1-docker.pkg.dev/proj/repo/agent:v1",
			ImageDigest: res.ImageDigest,
			LayerDirs:   res.LayerDirs,
			RefCount:    2,
		},
	}); err != nil {
		t.Fatalf("ReconcileImages: %v", err)
	}
	if srv.lastReconcile == nil || len(srv.lastReconcile.GetActiveImages()) != 1 || srv.lastReconcile.GetActiveImages()[0].GetRefCount() != 2 {
		t.Fatalf("unexpected ReconcileImagesRequest: %+v", srv.lastReconcile)
	}
}

func TestGRPCClientNotSupportedMapping(t *testing.T) {
	srv := &fakeServer{
		prepareErr: status.Error(codes.FailedPrecondition, "image not eligible for Riptide streaming"),
	}
	sock := startFakeServer(t, srv)
	client, err := ocilayer.NewGRPCClient(sock)
	if err != nil {
		t.Fatalf("NewGRPCClient: %v", err)
	}
	defer client.Close()

	_, err = client.PrepareImage(context.Background(), &ocilayer.PrepareRequest{
		ImageRef: "docker.io/library/busybox:latest",
	})
	if !errors.Is(err, ocilayer.ErrNotSupported) {
		t.Fatalf("PrepareImage err = %v, want errors.Is(..., ErrNotSupported)", err)
	}
}

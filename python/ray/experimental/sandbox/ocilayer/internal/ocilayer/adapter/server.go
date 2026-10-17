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

// Package adapter provides the shared OCILayerService gRPC server, reference
// count and lease lifecycle manager, and containerd Snapshots.v1 engine used by
// provider-specific adapter binaries (such as cmd/ocilayer-riptide and
// cmd/ocilayer-soci).
package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ocilayer"
	ocilayerpb "github.com/agent-substrate/substrate/internal/proto/ocilayer"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DefaultListenSocket is the standard Unix domain socket path where an
// OCILayerService adapter listens on the node.
const DefaultListenSocket = "/run/ate-image-streaming/ocilayer.sock"

// Server implements substrate.ocilayer.v1.OCILayerServiceServer by delegating
// to a provider Driver.
type Server struct {
	ocilayerpb.UnimplementedOCILayerServiceServer
	driver *Driver
}

// NewServer creates a new OCILayerService gRPC server backed by driver.
func NewServer(driver *Driver) *Server {
	return &Server{driver: driver}
}

// CanProvide checks whether the underlying snapshotter is reachable and able
// to serve the requested image.
func (s *Server) CanProvide(ctx context.Context, req *ocilayerpb.CanProvideRequest) (*ocilayerpb.CanProvideResponse, error) {
	if s.driver == nil {
		return &ocilayerpb.CanProvideResponse{CanProvide: false}, nil
	}
	ok, err := s.driver.CanProvide(ctx, &ocilayer.PrepareRequest{
		ImageRef: req.GetImageRef(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CanProvide: %v", err)
	}
	return &ocilayerpb.CanProvideResponse{
		CanProvide:   ok,
		ProviderName: s.driver.Name(),
	}, nil
}

// PrepareImage prepares and mounts all layers of the image, increments the
// image's reference count, and returns the ordered read-only host LayerDirs.
func (s *Server) PrepareImage(ctx context.Context, req *ocilayerpb.PrepareImageRequest) (*ocilayerpb.PrepareImageResponse, error) {
	if s.driver == nil {
		return nil, status.Error(codes.Unavailable, "no provider driver configured")
	}
	prepReq := &ocilayer.PrepareRequest{
		ImageRef: req.GetImageRef(),
	}
	if a := req.GetAuth(); a != nil {
		prepReq.AuthConfig = &ocilayer.AuthConfig{
			Username:      a.GetUsername(),
			Password:      a.GetPassword(),
			Auth:          a.GetAuth(),
			ServerAddress: a.GetServerAddress(),
			IdentityToken: a.GetIdentityToken(),
			RegistryToken: a.GetRegistryToken(),
		}
	}

	res, err := s.driver.PrepareImage(ctx, prepReq)
	if err != nil {
		if errors.Is(err, ocilayer.ErrNotSupported) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	var configJSON []byte
	if res.Config != nil {
		b, err := json.Marshal(res.Config)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshaling OCI image config: %v", err)
		}
		configJSON = b
	} else {
		b, _ := json.Marshal(&v1.Config{})
		configJSON = b
	}

	return &ocilayerpb.PrepareImageResponse{
		ImageDigest:  res.ImageDigest,
		ConfigJson:   configJSON,
		LayerDirs:    res.LayerDirs,
		ProviderName: s.driver.Name(),
	}, nil
}

// ReleaseImage decrements the reference count for the image's layers.
func (s *Server) ReleaseImage(ctx context.Context, req *ocilayerpb.ReleaseImageRequest) (*ocilayerpb.ReleaseImageResponse, error) {
	if s.driver == nil {
		return &ocilayerpb.ReleaseImageResponse{}, nil
	}
	if err := s.driver.ReleaseImage(ctx, &ocilayer.ReleaseRequest{
		ImageRef:  req.GetImageRef(),
		BypassTTL: req.GetBypassTtl(),
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &ocilayerpb.ReleaseImageResponse{}, nil
}

// ReconcileImages restores active reference counts from surviving actor
// bundles on atelet startup and sweeps orphaned views.
func (s *Server) ReconcileImages(ctx context.Context, req *ocilayerpb.ReconcileImagesRequest) (*ocilayerpb.ReconcileImagesResponse, error) {
	if s.driver == nil {
		return &ocilayerpb.ReconcileImagesResponse{}, nil
	}
	active := make([]*ocilayer.ActiveImageLayers, 0, len(req.GetActiveImages()))
	for _, img := range req.GetActiveImages() {
		if img == nil {
			continue
		}
		active = append(active, &ocilayer.ActiveImageLayers{
			ImageRef:    img.GetImageRef(),
			ImageDigest: img.GetImageDigest(),
			LayerDirs:   append([]string(nil), img.GetLayerDirs()...),
			RefCount:    int(img.GetRefCount()),
		})
	}
	if err := s.driver.ReconcileImages(ctx, active); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &ocilayerpb.ReconcileImagesResponse{}, nil
}

// ServeUnixSocket starts a gRPC server for driver on listenSocket and blocks
// until ctx is cancelled or the server returns an error.
func ServeUnixSocket(ctx context.Context, listenSocket string, driver *Driver) error {
	sockPath := strings.TrimPrefix(listenSocket, "unix://")
	if err := os.MkdirAll(filepath.Dir(sockPath), 0o755); err != nil {
		return fmt.Errorf("creating socket parent directory %s: %w", sockPath, err)
	}
	_ = os.Remove(sockPath)

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", sockPath, err)
	}
	defer func() {
		_ = lis.Close()
		_ = os.Remove(sockPath)
	}()

	grpcServer := grpc.NewServer()
	ocilayerpb.RegisterOCILayerServiceServer(grpcServer, NewServer(driver))

	go func() {
		<-ctx.Done()
		slog.Info("Shutting down OCILayerService server", slog.String("socket", sockPath))
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			grpcServer.Stop()
		}
	}()

	slog.Info("OCILayerService adapter listening",
		slog.String("provider", driver.Name()),
		slog.String("listenSocket", sockPath),
		slog.String("snapshotterSocket", driver.SocketPath()))

	return grpcServer.Serve(lis)
}

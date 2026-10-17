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

// Package ocilayer provides the vendor-agnostic client interface and gRPC
// implementation used by atelet to communicate with an out-of-process
// OCILayerService Adapter DaemonSet over a Unix domain socket.
package ocilayer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/agent-substrate/substrate/internal/proto/ocilayer"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ErrNotSupported reports that the OCILayerService adapter declined to serve
// an image (for example because the image has no streaming index or comes from
// an unsupported registry). Callers should fall back to local imagecache.
var ErrNotSupported = errors.New("image not supported by OCILayerService")

type contextKeyKeychain struct{}

// WithKeychainContext returns a child context carrying the specified authn.Keychain.
func WithKeychainContext(ctx context.Context, k authn.Keychain) context.Context {
	return context.WithValue(ctx, contextKeyKeychain{}, k)
}

// KeychainFromContext extracts an authn.Keychain from the context if present.
func KeychainFromContext(ctx context.Context) authn.Keychain {
	if ctx == nil {
		return nil
	}
	if k, ok := ctx.Value(contextKeyKeychain{}).(authn.Keychain); ok {
		return k
	}
	return nil
}

// AuthConfig contains resolved registry credentials for one image request.
type AuthConfig struct {
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	Auth          string `json:"auth,omitempty"`
	ServerAddress string `json:"serveraddress,omitempty"`
	IdentityToken string `json:"identitytoken,omitempty"`
	RegistryToken string `json:"registrytoken,omitempty"`
}

// PrepareRequest encapsulates the parameters for CanProvide and PrepareImage.
type PrepareRequest struct {
	// ImageRef is the OCI image reference.
	ImageRef string

	// AuthConfig holds explicit workload credentials (e.g. imagePullSecrets).
	// When nil, the client resolves credentials from KeychainFromContext(ctx)
	// if present.
	AuthConfig *AuthConfig
}

// PrepareResult holds the prepared read-only host layer directories for an OCI image.
type PrepareResult struct {
	// ImageDigest is the resolved manifest digest ("sha256:<hex>").
	ImageDigest string

	// Config is the decoded OCI image configuration (entrypoint, cmd, env, etc.).
	Config *v1.Config

	// LayerDirs contains the complete ordered list of read-only host layer
	// directories (bottom-most base layer first, matching OCI manifest order).
	LayerDirs []string

	// ProviderName is the underlying provider identifier reported by the adapter.
	ProviderName string

	// RootfsDir is the merged read-only root filesystem directory for the image.
	RootfsDir string
}

// ReleaseRequest encapsulates the parameters for ReleaseImage.
type ReleaseRequest struct {
	// ImageRef is the OCI image reference to release.
	ImageRef string

	// BypassTTL instructs the adapter to unmount layers immediately when
	// refCount reaches 0 rather than holding them for the idle retention TTL.
	BypassTTL bool
}

// ActiveImageLayers describes an active streamed image discovered from
// surviving actor bundle specs during atelet startup reconciliation.
type ActiveImageLayers struct {
	ImageRef    string
	ImageDigest string
	LayerDirs   []string
	RefCount    int
}

// Client is the interface used by atelet to interact with an OCILayerService adapter.
type Client interface {
	// Name returns the adapter/provider name ("ocilayer" or last reported provider name).
	Name() string

	// CanProvide checks whether the adapter is reachable and able to serve the image.
	CanProvide(ctx context.Context, req *PrepareRequest) (bool, error)

	// PrepareImage prepares and mounts all layers of the image on the host and
	// returns their ordered read-only directory paths. Returns an error wrapping
	// ErrNotSupported if the adapter declines the image.
	PrepareImage(ctx context.Context, req *PrepareRequest) (*PrepareResult, error)

	// ReleaseImage decrements the reference count for the image's layers.
	ReleaseImage(ctx context.Context, req *ReleaseRequest) error

	// ReconcileImages restores active reference counts on atelet startup and
	// unmounts orphaned layer views.
	ReconcileImages(ctx context.Context, active []*ActiveImageLayers) error
}

// GRPCClient implements Client over a Unix domain socket speaking
// substrate.ocilayer.v1.OCILayerService.
type GRPCClient struct {
	socketPath string

	mu           sync.Mutex
	conn         *grpc.ClientConn
	rpc          ocilayer.OCILayerServiceClient
	providerName string
}

// NewGRPCClient creates a new OCILayerService gRPC client targeting socketPath.
func NewGRPCClient(socketPath string) (*GRPCClient, error) {
	cleaned := strings.TrimSpace(socketPath)
	if cleaned == "" {
		return nil, errors.New("ocilayer: socket path must not be empty")
	}
	return &GRPCClient{
		socketPath:   cleaned,
		providerName: "ocilayer",
	}, nil
}

// SocketPath returns the configured Unix domain socket path.
func (c *GRPCClient) SocketPath() string {
	return c.socketPath
}

// Name returns the provider name reported by the adapter (defaults to "ocilayer").
func (c *GRPCClient) Name() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.providerName != "" {
		return c.providerName
	}
	return "ocilayer"
}

// Close closes the underlying gRPC connection if open.
func (c *GRPCClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.rpc = nil
		return err
	}
	return nil
}

func (c *GRPCClient) client() (ocilayer.OCILayerServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rpc != nil {
		return c.rpc, nil
	}
	target := c.socketPath
	if !strings.HasPrefix(target, "unix://") {
		target = "unix://" + target
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial ocilayer socket %s: %w", c.socketPath, err)
	}
	c.conn = conn
	c.rpc = ocilayer.NewOCILayerServiceClient(conn)
	return c.rpc, nil
}

func (c *GRPCClient) setProviderName(name string) {
	if name == "" {
		return
	}
	c.mu.Lock()
	c.providerName = name
	c.mu.Unlock()
}

// CanProvide checks whether the socket exists and the adapter can serve req.
func (c *GRPCClient) CanProvide(ctx context.Context, req *PrepareRequest) (bool, error) {
	if req == nil || strings.TrimSpace(req.ImageRef) == "" {
		return false, nil
	}
	rawPath := strings.TrimPrefix(c.socketPath, "unix://")
	if _, err := os.Stat(rawPath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	rpcClient, err := c.client()
	if err != nil {
		return false, err
	}
	resp, err := rpcClient.CanProvide(ctx, &ocilayer.CanProvideRequest{
		ImageRef: req.ImageRef,
		Auth:     resolveProtoAuth(ctx, req),
	})
	if err != nil {
		return false, err
	}
	c.setProviderName(resp.GetProviderName())
	return resp.GetCanProvide(), nil
}

// PrepareImage requests the adapter to prepare all layers for req.ImageRef.
func (c *GRPCClient) PrepareImage(ctx context.Context, req *PrepareRequest) (*PrepareResult, error) {
	if req == nil || strings.TrimSpace(req.ImageRef) == "" {
		return nil, fmt.Errorf("%w: empty image reference", ErrNotSupported)
	}
	rpcClient, err := c.client()
	if err != nil {
		return nil, err
	}
	resp, err := rpcClient.PrepareImage(ctx, &ocilayer.PrepareImageRequest{
		ImageRef: req.ImageRef,
		Auth:     resolveProtoAuth(ctx, req),
	})
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.FailedPrecondition, codes.Unimplemented, codes.NotFound:
				return nil, fmt.Errorf("%w: %s", ErrNotSupported, st.Message())
			}
		}
		return nil, err
	}
	if len(resp.GetLayerDirs()) == 0 {
		return nil, fmt.Errorf("%w: adapter returned zero layer_dirs for %s", ErrNotSupported, req.ImageRef)
	}
	var cfg *v1.Config
	if len(resp.GetConfigJson()) > 0 {
		var parsed v1.Config
		if err := json.Unmarshal(resp.GetConfigJson(), &parsed); err != nil {
			return nil, fmt.Errorf("decode image config JSON from adapter: %w", err)
		}
		cfg = &parsed
	}
	c.setProviderName(resp.GetProviderName())
	return &PrepareResult{
		ImageDigest:  resp.GetImageDigest(),
		Config:       cfg,
		LayerDirs:    resp.GetLayerDirs(),
		ProviderName: resp.GetProviderName(),
		RootfsDir:    resp.GetRootfsDir(),
	}, nil
}

// ReleaseImage decrements the reference count for req.ImageRef on the adapter.
func (c *GRPCClient) ReleaseImage(ctx context.Context, req *ReleaseRequest) error {
	if req == nil || strings.TrimSpace(req.ImageRef) == "" {
		return nil
	}
	rpcClient, err := c.client()
	if err != nil {
		return err
	}
	_, err = rpcClient.ReleaseImage(ctx, &ocilayer.ReleaseImageRequest{
		ImageRef:  req.ImageRef,
		BypassTtl: req.BypassTTL,
	})
	return err
}

// ReconcileImages sends the active streamed image list to the adapter.
func (c *GRPCClient) ReconcileImages(ctx context.Context, active []*ActiveImageLayers) error {
	rpcClient, err := c.client()
	if err != nil {
		return err
	}
	pbActive := make([]*ocilayer.ActiveImageLayers, 0, len(active))
	for _, a := range active {
		if a == nil {
			continue
		}
		pbActive = append(pbActive, &ocilayer.ActiveImageLayers{
			ImageRef:    a.ImageRef,
			ImageDigest: a.ImageDigest,
			LayerDirs:   a.LayerDirs,
			RefCount:    int32(a.RefCount),
		})
	}
	_, err = rpcClient.ReconcileImages(ctx, &ocilayer.ReconcileImagesRequest{
		ActiveImages: pbActive,
	})
	return err
}

func resolveProtoAuth(ctx context.Context, req *PrepareRequest) *ocilayer.RegistryAuth {
	if req == nil {
		return nil
	}
	if req.AuthConfig != nil {
		return &ocilayer.RegistryAuth{
			Username:      req.AuthConfig.Username,
			Password:      req.AuthConfig.Password,
			Auth:          req.AuthConfig.Auth,
			ServerAddress: req.AuthConfig.ServerAddress,
			IdentityToken: req.AuthConfig.IdentityToken,
			RegistryToken: req.AuthConfig.RegistryToken,
		}
	}
	kc := KeychainFromContext(ctx)
	if kc == nil || strings.TrimSpace(req.ImageRef) == "" {
		return nil
	}
	ref, err := name.ParseReference(req.ImageRef, name.WeakValidation)
	if err != nil {
		return nil
	}
	authenticator, err := kc.Resolve(ref.Context())
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
	return &ocilayer.RegistryAuth{
		Username:      cfg.Username,
		Password:      cfg.Password,
		Auth:          cfg.Auth,
		ServerAddress: ref.Context().RegistryStr(),
		IdentityToken: cfg.IdentityToken,
		RegistryToken: cfg.RegistryToken,
	}
}

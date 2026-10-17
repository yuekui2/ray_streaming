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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/agent-substrate/substrate/internal/ocilayer"
	"github.com/agent-substrate/substrate/internal/ocilayer/adapter"
	"github.com/agent-substrate/substrate/internal/proto/snapshots"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// ProviderSOCI identifies the AWS SOCI remote snapshotter provider.
	ProviderSOCI = "soci"

	// DefaultSOCISocket is the default UNIX socket path for soci-snapshotter-grpc.
	DefaultSOCISocket = "/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock"

	// DefaultSOCIDockerConfigPath is the default Docker config.json path shared
	// with soci-snapshotter-grpc (via DOCKER_CONFIG=/run/soci-snapshotter-grpc/docker)
	// for registry authentication.
	DefaultSOCIDockerConfigPath = "/run/soci-snapshotter-grpc/docker/config.json"

	// DefaultSOCIMaxLocalLayerSize is the default maximum compressed size (10 MiB,
	// matching soci create's default --min-layer-size) for a layer that returns
	// codes.OK from Snapshots.Prepare to be materialized locally. Layers larger
	// than this threshold that return codes.OK indicate an unindexed image and
	// fail fast with ocilayer.ErrNotSupported.
	DefaultSOCIMaxLocalLayerSize = 10 << 20
)

// LayerReaderFunc returns an uncompressed tar stream for a specific OCI layer.
type LayerReaderFunc func(ctx context.Context, imageRef string, auth *ocilayer.AuthConfig, layerIndex int, layerDigest string) (io.ReadCloser, error)

// SOCIConfig configures the AWS SOCI OCILayerService adapter.
type SOCIConfig struct {
	DockerConfigPath  string
	MaxLocalLayerSize int64
	LayerReader       LayerReaderFunc
	AdapterOpts       []adapter.Option
}

// SOCIDriver wraps the shared adapter.Driver with AWS SOCI's layer preparation
// loop: remote-streamed ztoc layers (codes.AlreadyExists) plus local tar unpack
// and Snapshots.Commit for small layers (< min-layer-size) that return codes.OK,
// and DOCKER_CONFIG credential synchronization for soci-snapshotter-grpc.
type SOCIDriver struct {
	*adapter.Driver
	dockerSyncer      *adapter.DockerConfigSyncer
	maxLocalLayerSize int64
	layerReader       LayerReaderFunc
}

// NewSOCI creates an AWS SOCI OCILayerService driver.
//
// Unlike Google Riptide v2 (which streams every layer of an imported GAR
// image), AWS SOCI skips indexing layers smaller than --min-layer-size (default
// 10 MiB). For those small layers, soci-snapshotter-grpc's Snapshots.Prepare
// returns codes.OK with a writable local snapshot directory instead of
// codes.AlreadyExists. To satisfy OCILayerService's all-or-nothing contract,
// the SOCI driver downloads and unpacks those small layers directly into the
// active snapshot directory and commits them under chainID so soci-snapshotter
// maintains a contiguous parent chain and returns all layer views.
func NewSOCI(cfg SOCIConfig) (*SOCIDriver, error) {
	maxLocal := cfg.MaxLocalLayerSize
	if maxLocal == 0 {
		maxLocal = DefaultSOCIMaxLocalLayerSize
	}
	sd := &SOCIDriver{
		dockerSyncer:      adapter.NewDockerConfigSyncer(cfg.DockerConfigPath),
		maxLocalLayerSize: maxLocal,
		layerReader:       cfg.LayerReader,
	}

	opts := append([]adapter.Option{
		adapter.WithLayerMounter(sd.mountLayers),
		adapter.WithWarmHitHook(sd.onWarmHit),
	}, cfg.AdapterOpts...)

	base, err := adapter.NewDriver(ProviderSOCI, DefaultSOCISocket, "soci", opts...)
	if err != nil {
		return nil, err
	}
	sd.Driver = base
	if sd.layerReader == nil {
		sd.layerReader = sd.defaultLayerReader
	}
	return sd, nil
}

func (sd *SOCIDriver) onWarmHit(ctx context.Context, _ string, req *ocilayer.PrepareRequest) {
	if req == nil {
		return
	}
	sd.dockerSyncer.Sync(ctx, sd.Driver.Keychain(), req.ImageRef, req.AuthConfig, false)
}

func (sd *SOCIDriver) mountLayers(ctx context.Context, env *adapter.MountEnv) (*adapter.MountedLayers, error) {
	sd.dockerSyncer.Sync(ctx, sd.Driver.Keychain(), env.ImageRef, env.AuthConfig, true)

	imgKey := adapter.SanitizePathKey(env.ImageRef)
	runID := fmt.Sprintf("%x", time.Now().UnixNano())
	nsCtx := env.Driver.WithNamespaceContext(ctx)

	res := &adapter.MountedLayers{
		MountDirs:    make([]string, 0, len(env.ChainInfos)),
		SnapshotKeys: make([]string, 0, len(env.ChainInfos)),
	}
	remoteStreamedCount := 0

	for i, c := range env.ChainInfos {
		prepKey := fmt.Sprintf("%s-%s-l%d-prep", imgKey, runID, i)
		viewKey := fmt.Sprintf("%s-%s-l%d-view", imgKey, runID, i)
		var layerDigest string
		if i < len(env.LayerDigests) {
			layerDigest = env.LayerDigests[i]
		}
		labels := adapter.StandardCRILayerLabels(env.CanonicalRef, env.ImageDigest, env.LayerDigests, i, c.ChainID)

		lk := env.Driver.AcquireChainLock(c.ChainID)
		lk.Lock()
		statStart := time.Now()
		committed, err := adapter.HasSnapshot(nsCtx, env.Client, env.Snapshotter, c.ChainID)
		res.StatDur += time.Since(statStart)
		streamedRemotely := committed
		if err == nil && !committed {
			prepStart := time.Now()
			prepResp, prepErr := env.Client.Prepare(nsCtx, &snapshots.PrepareSnapshotRequest{
				Snapshotter: env.Snapshotter,
				Key:         prepKey,
				Parent:      c.ParentChainID,
				Labels:      labels,
			})
			res.PrepareDur += time.Since(prepStart)
			switch status.Code(prepErr) {
			case codes.AlreadyExists:
				confirmStart := time.Now()
				found, sErr := adapter.HasSnapshot(nsCtx, env.Client, env.Snapshotter, c.ChainID)
				res.StatDur += time.Since(confirmStart)
				if sErr != nil {
					err = sErr
				} else if !found {
					err = errors.New("prepare returned AlreadyExists but the chain ID snapshot does not exist")
				} else {
					streamedRemotely = true
				}
			case codes.OK:
				if matErr := sd.materializeSmallLayer(ctx, env.Client, env.Snapshotter, env.ImageRef, env.AuthConfig, i, layerDigest, c, prepKey, prepResp.GetMounts()); matErr != nil {
					adapter.RemoveSnapshot(nsCtx, env.Client, env.Snapshotter, prepKey)
					err = matErr
				} else {
					res.CommittedKeys = append(res.CommittedKeys, c.ChainID)
				}
			default:
				adapter.RemoveSnapshot(nsCtx, env.Client, env.Snapshotter, prepKey)
				err = fmt.Errorf("preparing snapshot: %w", prepErr)
			}
		}
		lk.Unlock()
		if err != nil {
			return res, fmt.Errorf("layer %d (%s): %w", i, c.ChainID, err)
		}

		if streamedRemotely {
			remoteStreamedCount++
		}

		viewStart := time.Now()
		mounts, err := adapter.ViewLayerMounts(nsCtx, env.Client, env.Snapshotter, viewKey, c.ChainID)
		res.ViewDur += time.Since(viewStart)
		if err != nil {
			return res, fmt.Errorf("layer %d (%s): %w", i, c.ChainID, err)
		}
		res.SnapshotKeys = append(res.SnapshotKeys, viewKey)

		mountDir := adapter.ExtractMountDir(mounts)
		if mountDir == "" {
			return res, fmt.Errorf("unable to determine mount directory from mounts for layer %d (%s)", i, c.ChainID)
		}
		res.MountDirs = append(res.MountDirs, mountDir)
	}

	// If no layer in the image was remotely streamed by SOCI (e.g. an unindexed
	// image where every layer fell through to local unpack), decline the image
	// so atelet uses local imagecache.
	if remoteStreamedCount == 0 {
		return res, fmt.Errorf("no layers remotely streamed by %s: %w", ProviderSOCI, ocilayer.ErrNotSupported)
	}

	return res, nil
}

func (sd *SOCIDriver) materializeSmallLayer(ctx context.Context, client snapshots.SnapshotsClient, snapshotter, imageRef string, auth *ocilayer.AuthConfig, layerIndex int, layerDigest string, c adapter.ChainInfo, prepKey string, mounts []*snapshots.Mount) error {
	targetDir := adapter.ExtractActiveMountDir(mounts)
	if targetDir == "" {
		return fmt.Errorf("soci small layer %d (%s): no writable mount directory in Prepare response", layerIndex, c.ChainID)
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("creating soci active mount directory %s: %w", targetDir, err)
	}

	rc, err := sd.layerReader(ctx, imageRef, auth, layerIndex, layerDigest)
	if err != nil {
		return fmt.Errorf("fetching soci small layer %d (%s): %w", layerIndex, layerDigest, err)
	}
	defer func() { _ = rc.Close() }()

	if err := adapter.UnpackLayerTarToDir(ctx, rc, targetDir); err != nil {
		return fmt.Errorf("unpacking soci small layer %d (%s) into %s: %w", layerIndex, layerDigest, targetDir, err)
	}

	_, err = client.Commit(sd.Driver.WithNamespaceContext(ctx), &snapshots.CommitSnapshotRequest{
		Snapshotter: snapshotter,
		Name:        c.ChainID,
		Key:         prepKey,
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("committing soci small layer %d (%s): %w", layerIndex, c.ChainID, err)
	}

	slog.InfoContext(ctx, "Materialized small SOCI layer locally and committed snapshot",
		slog.Int("layerIndex", layerIndex),
		slog.String("layerDigest", layerDigest),
		slog.String("chainID", c.ChainID),
		slog.String("targetDir", targetDir))
	return nil
}

func (sd *SOCIDriver) checkLocalLayerSize(layerIndex int, layerDigest string, size int64) error {
	if sd.maxLocalLayerSize > 0 && size > sd.maxLocalLayerSize {
		return fmt.Errorf("layer %d (%s) compressed size %d exceeds max local layer size %d (image not indexed by soci): %w",
			layerIndex, layerDigest, size, sd.maxLocalLayerSize, ocilayer.ErrNotSupported)
	}
	return nil
}

func (sd *SOCIDriver) defaultLayerReader(ctx context.Context, imageRef string, authConfig *ocilayer.AuthConfig, layerIndex int, layerDigest string) (io.ReadCloser, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return nil, fmt.Errorf("parsing image reference %q: %w", imageRef, err)
	}
	opts := adapter.RemoteOptions(ctx, sd.Driver.Keychain(), authConfig)
	img, err := remote.Image(ref, opts...)
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
	layer := layers[layerIndex]
	if size, err := layer.Size(); err == nil {
		if err := sd.checkLocalLayerSize(layerIndex, layerDigest, size); err != nil {
			return nil, err
		}
	}
	return layer.Uncompressed()
}

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
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ocilayer"
	"github.com/agent-substrate/substrate/internal/proto/snapshots"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/google"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ChainInfo holds the content-addressed chainID and parent chainID for an OCI layer.
type ChainInfo struct {
	ChainID       string
	ParentChainID string
}

// StandardCRILayerLabels constructs the standard containerd CRI remote-snapshotter
// labels for a layer at layerIndex.
func StandardCRILayerLabels(canonicalRef, manifestDigest string, layerDigests []string, layerIndex int, chainID string) map[string]string {
	labels := map[string]string{
		"containerd.io/snapshot/remote":              "true",
		"containerd.io/snapshot.ref":                 chainID,
		"containerd.io/snapshot/cri.image-ref":       canonicalRef,
		"containerd.io/snapshot/cri.manifest-digest": manifestDigest,
	}
	if layerIndex >= 0 && layerIndex < len(layerDigests) {
		labels["containerd.io/snapshot/cri.layer-digest"] = layerDigests[layerIndex]
		if rem := strings.Join(layerDigests[layerIndex:], ","); rem != "" {
			labels["containerd.io/snapshot/cri.image-layers"] = rem
		}
	}
	return labels
}

// StandardMountLayers implements the standard 1-view-per-layer remote
// snapshotter preparation loop (Stat -> Prepare expecting codes.AlreadyExists ->
// View -> ExtractMountDir). Providers whose snapshotter daemon streams every
// layer as a read-only view (such as Google Riptide v2) can use this directly.
func StandardMountLayers(ctx context.Context, env *MountEnv) (*MountedLayers, error) {
	imgKey := SanitizePathKey(env.ImageRef)
	runID := fmt.Sprintf("%x", time.Now().UnixNano())
	nsCtx := env.Driver.WithNamespaceContext(ctx)

	res := &MountedLayers{
		MountDirs:    make([]string, 0, len(env.ChainInfos)),
		SnapshotKeys: make([]string, 0, len(env.ChainInfos)),
	}

	for i, c := range env.ChainInfos {
		prepKey := fmt.Sprintf("%s-%s-l%d-prep", imgKey, runID, i)
		viewKey := fmt.Sprintf("%s-%s-l%d-view", imgKey, runID, i)
		labels := StandardCRILayerLabels(env.CanonicalRef, env.ImageDigest, env.LayerDigests, i, c.ChainID)

		lk := env.Driver.AcquireChainLock(c.ChainID)
		lk.Lock()
		statStart := time.Now()
		committed, err := HasSnapshot(nsCtx, env.Client, env.Snapshotter, c.ChainID)
		res.StatDur += time.Since(statStart)
		if err == nil && !committed {
			prepStart := time.Now()
			_, prepErr := env.Client.Prepare(nsCtx, &snapshots.PrepareSnapshotRequest{
				Snapshotter: env.Snapshotter,
				Key:         prepKey,
				Parent:      c.ParentChainID,
				Labels:      labels,
			})
			res.PrepareDur += time.Since(prepStart)
			switch status.Code(prepErr) {
			case codes.AlreadyExists:
				confirmStart := time.Now()
				found, sErr := HasSnapshot(nsCtx, env.Client, env.Snapshotter, c.ChainID)
				res.StatDur += time.Since(confirmStart)
				if sErr != nil {
					err = sErr
				} else if !found {
					err = errors.New("prepare returned AlreadyExists but the chain ID snapshot does not exist")
				}
			case codes.OK:
				RemoveSnapshot(nsCtx, env.Client, env.Snapshotter, prepKey)
				err = fmt.Errorf("declined by snapshotter: %w", ocilayer.ErrNotSupported)
			default:
				RemoveSnapshot(nsCtx, env.Client, env.Snapshotter, prepKey)
				err = fmt.Errorf("preparing snapshot: %w", prepErr)
			}
		}
		lk.Unlock()
		if err != nil {
			return res, fmt.Errorf("layer %d (%s): %w", i, c.ChainID, err)
		}

		viewStart := time.Now()
		mounts, err := ViewLayerMounts(nsCtx, env.Client, env.Snapshotter, viewKey, c.ChainID)
		res.ViewDur += time.Since(viewStart)
		if err != nil {
			return res, fmt.Errorf("layer %d (%s): %w", i, c.ChainID, err)
		}
		res.SnapshotKeys = append(res.SnapshotKeys, viewKey)

		mountDir := ExtractMountDir(mounts)
		if mountDir == "" {
			return res, fmt.Errorf("unable to determine mount directory from mounts for layer %d (%s)", i, c.ChainID)
		}
		res.MountDirs = append(res.MountDirs, mountDir)
	}
	return res, nil
}

// HasSnapshot reports whether a snapshot key exists in the snapshotter.
// The caller should pass a namespace-annotated context (via Driver.WithNamespaceContext).
func HasSnapshot(ctx context.Context, client snapshots.SnapshotsClient, snapshotter, key string) (bool, error) {
	_, err := client.Stat(ctx, &snapshots.StatSnapshotRequest{
		Snapshotter: snapshotter,
		Key:         key,
	})
	switch status.Code(err) {
	case codes.OK:
		return true, nil
	case codes.NotFound:
		return false, nil
	default:
		return false, fmt.Errorf("stat snapshot %q: %w", key, err)
	}
}

// RemoveSnapshot best-effort removes a snapshot key from the snapshotter.
// The caller should pass a namespace-annotated context (via Driver.WithNamespaceContext).
func RemoveSnapshot(ctx context.Context, client snapshots.SnapshotsClient, snapshotter, key string) {
	_, _ = client.Remove(ctx, &snapshots.RemoveSnapshotRequest{
		Snapshotter: snapshotter,
		Key:         key,
	})
}

// ViewLayerMounts creates a read-only snapshot view for parentChainID and
// returns its Mount descriptors.
func ViewLayerMounts(ctx context.Context, client snapshots.SnapshotsClient, snapshotter, viewKey, parentChainID string) ([]*snapshots.Mount, error) {
	resp, err := client.View(ctx, &snapshots.ViewSnapshotRequest{
		Snapshotter: snapshotter,
		Key:         viewKey,
		Parent:      parentChainID,
	})
	if err != nil {
		RemoveSnapshot(ctx, client, snapshotter, viewKey)
		return nil, fmt.Errorf("creating view: %w", err)
	}
	if len(resp.GetMounts()) == 0 {
		RemoveSnapshot(ctx, client, snapshotter, viewKey)
		return nil, fmt.Errorf("no mounts returned for snapshot %s", parentChainID)
	}
	return resp.GetMounts(), nil
}

// ExtractMountDir extracts the read-only lowerdir or bind source directory from
// a slice of snapshotter Mount descriptors.
func ExtractMountDir(mounts []*snapshots.Mount) string {
	if len(mounts) == 0 {
		return ""
	}
	for _, m := range mounts {
		if m == nil {
			continue
		}
		if m.Source != "" && m.Source != "overlay" && m.Source != "none" {
			return m.Source
		}
		for _, opt := range m.Options {
			if strings.HasPrefix(opt, "lowerdir=") {
				parts := strings.Split(strings.TrimPrefix(opt, "lowerdir="), ":")
				if len(parts) > 0 && parts[0] != "" {
					return parts[0]
				}
			}
		}
	}
	return mounts[0].Source
}

// ExtractActiveMountDir extracts the writable directory (overlay upperdir or
// bind source) from a slice of active snapshot Mount descriptors returned by
// Snapshots.Prepare.
func ExtractActiveMountDir(mounts []*snapshots.Mount) string {
	for _, m := range mounts {
		if m == nil {
			continue
		}
		for _, opt := range m.Options {
			if dir, ok := strings.CutPrefix(opt, "upperdir="); ok && dir != "" {
				return dir
			}
		}
	}
	return ExtractMountDir(mounts)
}

// ComputeChainIDs computes OCI ChainIDs for an ordered slice of layer DiffIDs.
func ComputeChainIDs(diffIDs []string) []ChainInfo {
	res := make([]ChainInfo, len(diffIDs))
	parent := ""
	for i, diff := range diffIDs {
		var current string
		if parent == "" {
			current = diff
		} else {
			h := sha256.Sum256([]byte(parent + " " + diff))
			current = "sha256:" + hex.EncodeToString(h[:])
		}
		res[i] = ChainInfo{
			ChainID:       current,
			ParentChainID: parent,
		}
		parent = current
	}
	return res
}

// SanitizePathKey converts an OCI image reference into a filesystem-safe directory name.
func SanitizePathKey(ref string) string {
	s := strings.ReplaceAll(ref, "/", "_")
	s = strings.ReplaceAll(s, ":", "_")
	s = strings.ReplaceAll(s, "@", "_")
	return s
}

// CanonicalImageRef returns <repo>@<digest> when digest is non-empty and
// imageRef is tag-based.
func CanonicalImageRef(imageRef, digest string) string {
	if digest == "" || strings.Contains(imageRef, "@") {
		return imageRef
	}
	if ref, err := name.ParseReference(imageRef); err == nil {
		return ref.Context().Name() + "@" + digest
	}
	return imageRef
}

// RemoteOptions builds go-containerregistry remote.Option values using explicit
// AuthConfig credentials when provided or falling back to keychain.
func RemoteOptions(ctx context.Context, keychain authn.Keychain, authConfig *ocilayer.AuthConfig) []remote.Option {
	opts := []remote.Option{remote.WithContext(ctx)}
	if authConfig != nil && (authConfig.Username != "" || authConfig.Password != "" || authConfig.Auth != "" || authConfig.IdentityToken != "" || authConfig.RegistryToken != "") {
		var auth authn.Authenticator
		if authConfig.Username != "" || authConfig.Password != "" {
			auth = &authn.Basic{
				Username: authConfig.Username,
				Password: authConfig.Password,
			}
		} else if authConfig.Auth != "" {
			auth = authn.FromConfig(authn.AuthConfig{
				Auth: authConfig.Auth,
			})
		} else if authConfig.RegistryToken != "" {
			auth = &authn.Bearer{Token: authConfig.RegistryToken}
		} else if authConfig.IdentityToken != "" {
			auth = &authn.Bearer{Token: authConfig.IdentityToken}
		}
		if auth != nil {
			opts = append(opts, remote.WithAuth(auth))
		}
	} else if keychain != nil {
		opts = append(opts, remote.WithAuthFromKeychain(keychain))
	} else {
		opts = append(opts, remote.WithAuthFromKeychain(authn.NewMultiKeychain(authn.DefaultKeychain, google.Keychain)))
	}
	return opts
}

const (
	tarWhiteoutPrefix   = ".wh."
	tarOpaqueMarkerName = ".wh..wh..opq"
)

func validateLayerTarPath(p string) (cleaned string, skip bool, err error) {
	if p == "" {
		return "", true, nil
	}
	cleaned = filepath.Clean(p)
	if cleaned == "." {
		return "", true, nil
	}
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "" || cleaned == "." {
		return "", true, nil
	}
	if !filepath.IsLocal(cleaned) {
		return "", false, fmt.Errorf("non-local tar entry path %q", p)
	}
	return cleaned, false, nil
}

// UnpackLayerTarToDir safely extracts an uncompressed OCI layer tar stream into
// targetDir using os.OpenRoot to prevent path traversal outside targetDir.
func UnpackLayerTarToDir(ctx context.Context, r io.Reader, targetDir string) error {
	root, err := os.OpenRoot(targetDir)
	if err != nil {
		return fmt.Errorf("opening root %q: %w", targetDir, err)
	}
	defer func() { _ = root.Close() }()

	dirModes := make(map[string]os.FileMode)
	tr := tar.NewReader(r)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar header: %w", err)
		}

		entryName, skip, err := validateLayerTarPath(hdr.Name)
		if err != nil {
			return err
		}
		if skip {
			continue
		}

		base := filepath.Base(entryName)
		if base == tarOpaqueMarkerName || strings.HasPrefix(base, tarWhiteoutPrefix+tarWhiteoutPrefix) {
			continue
		}
		if deleted, ok := strings.CutPrefix(base, tarWhiteoutPrefix); ok {
			target := filepath.Join(filepath.Dir(entryName), deleted)
			_ = root.RemoveAll(target)
			continue
		}

		if parent := filepath.Dir(entryName); parent != "." {
			if err := root.MkdirAll(parent, 0o755); err != nil {
				return fmt.Errorf("creating parent directory %q: %w", parent, err)
			}
		}

		mode := hdr.FileInfo().Mode().Perm()
		switch hdr.Typeflag {
		case tar.TypeReg, tar.TypeRegA:
			if _, err := root.Lstat(entryName); err == nil {
				if err := root.RemoveAll(entryName); err != nil {
					return fmt.Errorf("removing existing path %q: %w", entryName, err)
				}
			}
			f, err := root.OpenFile(entryName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
			if err != nil {
				return fmt.Errorf("creating file %q: %w", entryName, err)
			}
			_, copyErr := io.Copy(f, tr)
			closeErr := f.Close()
			if copyErr != nil {
				return fmt.Errorf("writing file %q: %w", entryName, copyErr)
			}
			if closeErr != nil {
				return fmt.Errorf("closing file %q: %w", entryName, closeErr)
			}

		case tar.TypeDir:
			if err := root.Mkdir(entryName, mode|0o700); err != nil && !errors.Is(err, os.ErrExist) {
				return fmt.Errorf("creating directory %q: %w", entryName, err)
			}
			dirModes[entryName] = mode

		case tar.TypeSymlink:
			if _, err := root.Lstat(entryName); err == nil {
				if err := root.RemoveAll(entryName); err != nil {
					return fmt.Errorf("removing existing path before symlink %q: %w", entryName, err)
				}
			}
			if err := root.Symlink(hdr.Linkname, entryName); err != nil {
				return fmt.Errorf("creating symlink %q -> %q: %w", entryName, hdr.Linkname, err)
			}

		case tar.TypeLink:
			linkTarget, linkSkip, err := validateLayerTarPath(hdr.Linkname)
			if err != nil {
				return err
			}
			if linkSkip {
				return fmt.Errorf("empty hardlink target for %q", entryName)
			}
			if _, err := root.Lstat(entryName); err == nil {
				if err := root.RemoveAll(entryName); err != nil {
					return fmt.Errorf("removing existing path before hardlink %q: %w", entryName, err)
				}
			}
			if err := root.Link(linkTarget, entryName); err != nil {
				return fmt.Errorf("creating hardlink %q -> %q: %w", entryName, linkTarget, err)
			}
		}
	}

	dirs := make([]string, 0, len(dirModes))
	for dName := range dirModes {
		dirs = append(dirs, dName)
	}
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, dName := range dirs {
		if err := root.Chmod(dName, dirModes[dName]); err != nil {
			return fmt.Errorf("restoring mode on directory %q: %w", dName, err)
		}
	}
	return nil
}

const defaultDockerCredsCacheTTL = 5 * time.Minute

type dockerConfigFile struct {
	Auths map[string]dockerAuthEntry `json:"auths"`
}

type dockerAuthEntry struct {
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	Auth          string `json:"auth,omitempty"`
	IdentityToken string `json:"identitytoken,omitempty"`
	RegistryToken string `json:"registrytoken,omitempty"`
}

type pushedDockerCredEntry struct {
	digest   [32]byte
	pushedAt time.Time
}

// DockerConfigSyncer synchronizes registry credentials into a Docker config.json
// file shared with snapshotter daemons that read DOCKER_CONFIG (such as
// soci-snapshotter-grpc and containerd-nydus-grpc).
type DockerConfigSyncer struct {
	path string
	mu   sync.Mutex
	sent map[string]pushedDockerCredEntry
}

// NewDockerConfigSyncer creates a DockerConfigSyncer writing to configPath.
func NewDockerConfigSyncer(configPath string) *DockerConfigSyncer {
	return &DockerConfigSyncer{
		path: configPath,
		sent: make(map[string]pushedDockerCredEntry),
	}
}

// Sync resolves credentials for imageRef from reqAuth or keychain and atomically
// updates the Docker config.json file if the credentials changed or expired.
func (s *DockerConfigSyncer) Sync(ctx context.Context, keychain authn.Keychain, imageRef string, reqAuth *ocilayer.AuthConfig, force bool) {
	if s == nil || s.path == "" || imageRef == "" {
		return
	}
	serverAddr, entry := resolveDockerAuth(keychain, imageRef, reqAuth)
	if serverAddr == "" || entry == nil {
		return
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{
		serverAddr,
		entry.Username,
		entry.Password,
		entry.Auth,
		entry.IdentityToken,
		entry.RegistryToken,
	}, "\x00")))
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	if !force {
		if prev, ok := s.sent[serverAddr]; ok && prev.digest == digest && now.Sub(prev.pushedAt) < defaultDockerCredsCacheTTL {
			return
		}
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		slog.WarnContext(ctx, "Failed to create docker config directory",
			slog.String("path", dir),
			slog.Any("error", err))
		return
	}

	var cfg dockerConfigFile
	if b, err := os.ReadFile(s.path); err == nil && len(b) > 0 {
		_ = json.Unmarshal(b, &cfg)
	}
	if cfg.Auths == nil {
		cfg.Auths = make(map[string]dockerAuthEntry)
	}
	cfg.Auths[serverAddr] = *entry

	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return
	}
	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, out, 0o600); err != nil {
		slog.WarnContext(ctx, "Failed to write temporary docker config",
			slog.String("path", tmpPath),
			slog.Any("error", err))
		return
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		slog.WarnContext(ctx, "Failed to rename docker config",
			slog.String("path", s.path),
			slog.Any("error", err))
		return
	}
	if s.sent == nil {
		s.sent = make(map[string]pushedDockerCredEntry)
	}
	s.sent[serverAddr] = pushedDockerCredEntry{
		digest:   digest,
		pushedAt: now,
	}
	slog.InfoContext(ctx, "Synchronized registry credentials to docker config",
		slog.String("path", s.path),
		slog.String("image", imageRef),
		slog.String("server", serverAddr),
		slog.String("username", entry.Username))
}

func resolveDockerAuth(keychain authn.Keychain, refStr string, reqAuth *ocilayer.AuthConfig) (string, *dockerAuthEntry) {
	ref, err := name.ParseReference(refStr)
	var serverAddr string
	if err == nil {
		serverAddr = ref.Context().RegistryStr()
	}
	if reqAuth != nil && (reqAuth.Username != "" || reqAuth.Password != "" || reqAuth.Auth != "" || reqAuth.IdentityToken != "" || reqAuth.RegistryToken != "") {
		if reqAuth.ServerAddress != "" {
			serverAddr = reqAuth.ServerAddress
		}
		if serverAddr == "" {
			return "", nil
		}
		entry := &dockerAuthEntry{
			Username:      reqAuth.Username,
			Password:      reqAuth.Password,
			Auth:          reqAuth.Auth,
			IdentityToken: reqAuth.IdentityToken,
			RegistryToken: reqAuth.RegistryToken,
		}
		if entry.Auth == "" && (entry.Username != "" || entry.Password != "") {
			entry.Auth = base64.StdEncoding.EncodeToString([]byte(entry.Username + ":" + entry.Password))
		}
		return serverAddr, entry
	}
	if keychain == nil || err != nil {
		return "", nil
	}
	authenticator, err := keychain.Resolve(ref.Context())
	if err != nil || authenticator == nil || authenticator == authn.Anonymous {
		return "", nil
	}
	cfg, err := authenticator.Authorization()
	if err != nil || cfg == nil {
		return "", nil
	}
	if cfg.Username == "" && cfg.Password == "" && cfg.Auth == "" && cfg.IdentityToken == "" && cfg.RegistryToken == "" {
		return "", nil
	}
	entry := &dockerAuthEntry{
		Username:      cfg.Username,
		Password:      cfg.Password,
		Auth:          cfg.Auth,
		IdentityToken: cfg.IdentityToken,
		RegistryToken: cfg.RegistryToken,
	}
	if entry.Auth == "" && (entry.Username != "" || entry.Password != "") {
		entry.Auth = base64.StdEncoding.EncodeToString([]byte(entry.Username + ":" + entry.Password))
	}
	return serverAddr, entry
}

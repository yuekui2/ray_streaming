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
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/agent-substrate/substrate/internal/ocilayer"
	"github.com/agent-substrate/substrate/internal/ocilayer/adapter"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/klauspost/compress/zstd"
)

func TestResolveSpegelEndpoints(t *testing.T) {
	t.Setenv(EnvSpegelMirrorAddr, "")
	t.Setenv(EnvSpegelRouterAddr, "")

	mirror, err := ResolveSpegelMirrorAddr("")
	if err != nil {
		t.Fatalf("ResolveSpegelMirrorAddr default failed: %v", err)
	}
	if mirror != DefaultSpegelMirrorAddr {
		t.Fatalf("got %q, want %q", mirror, DefaultSpegelMirrorAddr)
	}

	router, err := ResolveSpegelRouterAddr("")
	if err != nil {
		t.Fatalf("ResolveSpegelRouterAddr default failed: %v", err)
	}
	if router != DefaultSpegelRouterAddr {
		t.Fatalf("got %q, want %q", router, DefaultSpegelRouterAddr)
	}

	t.Setenv(EnvSpegelMirrorAddr, "http://127.0.0.1:15000/")
	mirror, err = ResolveSpegelMirrorAddr("")
	if err != nil || mirror != "http://127.0.0.1:15000" {
		t.Fatalf("env override failed: mirror=%q err=%v", mirror, err)
	}

	if _, err := ResolveSpegelMirrorAddr("ftp://bad-scheme"); err == nil {
		t.Fatalf("expected error for invalid scheme")
	}
	if _, err := ResolveSpegelRouterAddr("missing-port"); err == nil {
		t.Fatalf("expected error for missing port")
	}
}

func TestSpegelHealthProbeAndCanProvide(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(false)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			if healthy.Load() {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok"))
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	tmp := t.TempDir()
	sd, err := NewSpegel(SpegelConfig{
		StoreDir:          filepath.Join(tmp, "store"),
		DockerConfigPath:  filepath.Join(tmp, "docker", "config.json"),
		SpegelMirrorAddr:  srv.URL,
		VerifySpegelReady: true,
		AdapterOpts: []adapter.Option{
			adapter.WithWorkDir(filepath.Join(tmp, "work")),
		},
	})
	if err != nil {
		t.Fatalf("NewSpegel failed: %v", err)
	}
	defer func() { _ = sd.Close() }()

	ctx := context.Background()
	ok, err := sd.CanProvide(ctx, &ocilayer.PrepareRequest{ImageRef: "us-central1-docker.pkg.dev/proj/repo/app:v1"})
	if err != nil {
		t.Fatalf("CanProvide returned unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected CanProvide=false when Spegel /healthz returns 503")
	}

	healthy.Store(true)
	ok, err = sd.CanProvide(ctx, &ocilayer.PrepareRequest{ImageRef: "us-central1-docker.pkg.dev/proj/repo/app:v1"})
	if err != nil {
		t.Fatalf("CanProvide returned unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected CanProvide=true when Spegel /healthz returns 200")
	}
}

func TestSpegelCrossNodeP2PFlow(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	// Build two test layers: Layer 0 (gzip) and Layer 1 (zstd).
	tar0, diff0 := buildTestTar(t, map[string]string{
		"etc/os-release": "NAME=Ubuntu\nVERSION=24.04\n",
		"bin/base":       "base-binary-payload",
	})
	gzBlob0, blobDigest0 := compressGzip(t, tar0)

	tar1, diff1 := buildTestTar(t, map[string]string{
		"app/agent.py": "print('hello from spegel p2p')\n",
	})
	zstdBlob1, blobDigest1 := compressZstd(t, tar1)

	testImage := "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/ate-e2e/agent:v1"
	imageDigest := "sha256:9999999999999999999999999999999999999999999999999999999999999999"

	resolver := func(_ context.Context, _ string, _ *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error) {
		return imageDigest, &v1.Config{Env: []string{"PATH=/usr/bin:/bin"}}, []string{diff0, diff1}, []string{blobDigest0, blobDigest1}, nil
	}

	var nodeAUpstreamCalls atomic.Int64
	nodeAFetcher := func(_ context.Context, _ string, _ *ocilayer.AuthConfig, layerIndex int, _ string) (io.ReadCloser, error) {
		nodeAUpstreamCalls.Add(1)
		switch layerIndex {
		case 0:
			return io.NopCloser(bytes.NewReader(gzBlob0)), nil
		case 1:
			return io.NopCloser(bytes.NewReader(zstdBlob1)), nil
		default:
			return nil, fmt.Errorf("unexpected layer index %d", layerIndex)
		}
	}

	// Node A: starts its embedded SpegelPeerServer on an ephemeral port.
	nodeA, err := NewSpegel(SpegelConfig{
		StoreDir:            filepath.Join(tmp, "nodeA-store"),
		DockerConfigPath:    filepath.Join(tmp, "nodeA-docker", "config.json"),
		EnableP2PServer:     true,
		P2PListenAddr:       "127.0.0.1:0",
		SpegelBootstrapDNS:  "nonexistent.invalid",
		VerifySpegelReady:   true,
		UpstreamBlobFetcher: nodeAFetcher,
		AdapterOpts: []adapter.Option{
			adapter.WithWorkDir(filepath.Join(tmp, "nodeA-work")),
			adapter.WithImageResolver(resolver),
		},
	})
	if err != nil {
		t.Fatalf("NewSpegel(nodeA) failed: %v", err)
	}
	defer func() { _ = nodeA.Close() }()

	// Step 1: Cold PrepareImage on Node A pulls from upstream and populates Node A's Spegel blob store.
	resA, err := nodeA.PrepareImage(ctx, &ocilayer.PrepareRequest{
		ImageRef: testImage,
		AuthConfig: &ocilayer.AuthConfig{
			Username: "oauth2accesstoken",
			Password: "test-gar-token",
		},
	})
	if err != nil {
		t.Fatalf("nodeA.PrepareImage failed: %v", err)
	}
	if len(resA.LayerDirs) != 2 {
		t.Fatalf("got %d LayerDirs on Node A, want 2", len(resA.LayerDirs))
	}
	if nodeAUpstreamCalls.Load() != 2 || nodeA.UpstreamBlobMisses() != 2 {
		t.Fatalf("expected 2 upstream fetches on cold Node A, got calls=%d misses=%d", nodeAUpstreamCalls.Load(), nodeA.UpstreamBlobMisses())
	}

	// Node B: configured with Node A as a cluster Spegel peer, and with its
	// upstream registry fetcher disabled (failing) to prove 100% P2P transfer!
	nodeB, err := NewSpegel(SpegelConfig{
		StoreDir:           filepath.Join(tmp, "nodeB-store"),
		DockerConfigPath:   filepath.Join(tmp, "nodeB-docker", "config.json"),
		EnableP2PServer:    true,
		P2PListenAddr:      "127.0.0.1:0",
		SpegelBootstrapDNS: "nonexistent.invalid",
		SpegelPeers:        []string{nodeA.PeerServer().Addr()},
		VerifySpegelReady:  true,
		UpstreamBlobFetcher: func(_ context.Context, _ string, _ *ocilayer.AuthConfig, _ int, _ string) (io.ReadCloser, error) {
			return nil, errors.New("upstream registry intentionally offline on Node B")
		},
		AdapterOpts: []adapter.Option{
			adapter.WithWorkDir(filepath.Join(tmp, "nodeB-work")),
			adapter.WithImageResolver(resolver),
		},
	})
	if err != nil {
		t.Fatalf("NewSpegel(nodeB) failed: %v", err)
	}
	defer func() { _ = nodeB.Close() }()

	// Step 2: PrepareImage on Node B streams both compressed blobs peer-to-peer from Node A!
	resB, err := nodeB.PrepareImage(ctx, &ocilayer.PrepareRequest{
		ImageRef: testImage,
	})
	if err != nil {
		t.Fatalf("nodeB.PrepareImage (P2P from Node A) failed: %v", err)
	}
	if len(resB.LayerDirs) != 2 {
		t.Fatalf("got %d LayerDirs on Node B, want 2", len(resB.LayerDirs))
	}
	if nodeB.P2PBlobHits() != 2 || nodeB.UpstreamBlobMisses() != 0 {
		t.Fatalf("expected P2PBlobHits=2 and UpstreamBlobMisses=0 on Node B, got hits=%d misses=%d",
			nodeB.P2PBlobHits(), nodeB.UpstreamBlobMisses())
	}

	statsA := nodeA.PeerServer().Stats()
	if statsA.P2PRequestsIn < 2 || statsA.P2PBytesServed != int64(len(gzBlob0)+len(zstdBlob1)) {
		t.Fatalf("unexpected Node A P2P server stats: %+v (want P2PBytesServed=%d)", statsA, len(gzBlob0)+len(zstdBlob1))
	}

	gotAgentPy, err := os.ReadFile(filepath.Join(resB.LayerDirs[1], "fs", "app", "agent.py"))
	if err != nil || string(gotAgentPy) != "print('hello from spegel p2p')\n" {
		t.Fatalf("unexpected file content on Node B layer-1: %q (err=%v)", string(gotAgentPy), err)
	}
}

func TestSpegelSharedLayerDeduplicationAndCorruptBlobFallback(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	tar0, diff0 := buildTestTar(t, map[string]string{
		"etc/hosts": "127.0.0.1 localhost\n",
	})
	gzBlob0, blobDigest0 := compressGzip(t, tar0)

	// Corrupt peer mirror that returns invalid bytes for blobDigest0.
	corruptMirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("corrupted-blob-bytes"))
	}))
	defer corruptMirror.Close()

	var upstreamCalls atomic.Int64
	sd, err := NewSpegel(SpegelConfig{
		StoreDir:          filepath.Join(tmp, "store"),
		DockerConfigPath:  filepath.Join(tmp, "docker", "config.json"),
		SpegelMirrorAddr:  corruptMirror.URL,
		VerifySpegelReady: true,
		UpstreamBlobFetcher: func(_ context.Context, _ string, _ *ocilayer.AuthConfig, _ int, _ string) (io.ReadCloser, error) {
			upstreamCalls.Add(1)
			return io.NopCloser(bytes.NewReader(gzBlob0)), nil
		},
		AdapterOpts: []adapter.Option{
			adapter.WithWorkDir(filepath.Join(tmp, "work")),
			adapter.WithImageResolver(func(_ context.Context, ref string, _ *ocilayer.AuthConfig) (string, *v1.Config, []string, []string, error) {
				if ref == "us-central1-docker.pkg.dev/p/r/img1:v1" {
					return "sha256:1111111111111111111111111111111111111111111111111111111111111111", &v1.Config{}, []string{diff0}, []string{blobDigest0}, nil
				}
				return "sha256:2222222222222222222222222222222222222222222222222222222222222222", &v1.Config{}, []string{diff0}, []string{blobDigest0}, nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("NewSpegel failed: %v", err)
	}
	defer func() { _ = sd.Close() }()

	// Image 1: corrupt P2P blob is rejected and falls back cleanly to upstream.
	if _, err := sd.PrepareImage(ctx, &ocilayer.PrepareRequest{ImageRef: "us-central1-docker.pkg.dev/p/r/img1:v1"}); err != nil {
		t.Fatalf("PrepareImage(img1) failed: %v", err)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("expected 1 upstream call after corrupt P2P fallback, got %d", upstreamCalls.Load())
	}

	// Image 2 sharing diff0: reuses already-unpacked layer on disk with 0 extra fetches.
	if _, err := sd.PrepareImage(ctx, &ocilayer.PrepareRequest{ImageRef: "us-central1-docker.pkg.dev/p/r/img2:v1"}); err != nil {
		t.Fatalf("PrepareImage(img2) failed: %v", err)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("expected shared layer to reuse on-disk unpacked layer without extra upstream calls, got %d", upstreamCalls.Load())
	}
}

func buildTestTar(t *testing.T, files map[string]string) ([]byte, string) {
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
			t.Fatalf("WriteHeader(%s): %v", name, err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatalf("Write(%s): %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar.Close: %v", err)
	}
	raw := buf.Bytes()
	sum := sha256.Sum256(raw)
	return raw, "sha256:" + hex.EncodeToString(sum[:])
}

func compressGzip(t *testing.T, raw []byte) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(raw); err != nil {
		t.Fatalf("gzip.Write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip.Close: %v", err)
	}
	compressed := buf.Bytes()
	sum := sha256.Sum256(compressed)
	return compressed, "sha256:" + hex.EncodeToString(sum[:])
}

func compressZstd(t *testing.T, raw []byte) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatalf("zstd.NewWriter: %v", err)
	}
	if _, err := zw.Write(raw); err != nil {
		t.Fatalf("zstd.Write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zstd.Close: %v", err)
	}
	compressed := buf.Bytes()
	sum := sha256.Sum256(compressed)
	return compressed, "sha256:" + hex.EncodeToString(sum[:])
}

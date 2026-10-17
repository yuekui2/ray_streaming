from concurrent import futures
import json
import os
import threading
from typing import Dict, Optional
from unittest.mock import patch

import grpc
import pytest

from ray.experimental.sandbox._internal import ocilayer_pb2, ocilayer_pb2_grpc
from ray.experimental.sandbox.image_manager import (
    STREAMED_MARKER,
    ImageManager,
)


def _minimal_base_spec() -> dict:
    return {
        "ociVersion": "1.0.0",
        "process": {"args": ["sh"], "env": [], "cwd": "/"},
        "root": {"path": "rootfs", "readonly": True},
        "mounts": [],
        "linux": {"namespaces": [{"type": "network"}]},
    }


class FakeOCILayerServicer(ocilayer_pb2_grpc.OCILayerServiceServicer):
    """In-process fake OCILayerService gRPC servicer for unit tests."""

    def __init__(self, rootfs_base_dir: str, provider_name: str = "fake-soci"):
        self._rootfs_base_dir = rootfs_base_dir
        self._provider_name = provider_name
        self._lock = threading.Lock()
        self.can_provide_result: bool = True
        self.fail_precondition_images: set[str] = set()
        self.ref_counts: Dict[str, int] = {}
        self.prepare_calls: list[str] = []
        self.release_calls: list[str] = []
        self.config_by_image: Dict[str, dict] = {}

    def get_rootfs_dir(self, image_ref: str) -> str:
        safe = image_ref.replace("/", "_").replace(":", "_")
        path = os.path.join(self._rootfs_base_dir, safe, "rootfs")
        os.makedirs(path, exist_ok=True)
        marker = os.path.join(path, "hello.txt")
        if not os.path.exists(marker):
            with open(marker, "w", encoding="utf-8") as f:
                f.write("streamed-rootfs")
        return path

    def CanProvide(self, request, context):
        return ocilayer_pb2.CanProvideResponse(
            can_provide=self.can_provide_result,
            provider_name=self._provider_name,
        )

    def PrepareImage(self, request, context):
        image_ref = request.image_ref
        with self._lock:
            self.prepare_calls.append(image_ref)
            if image_ref in self.fail_precondition_images:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"image {image_ref} not supported by provider {self._provider_name}",
                )
            self.ref_counts[image_ref] = self.ref_counts.get(image_ref, 0) + 1
            cfg = self.config_by_image.get(
                image_ref,
                {
                    "WorkingDir": "/workspace/agent",
                    "Env": ["PATH=/usr/local/bin:/usr/bin", "STREAMED_VAR=1"],
                },
            )
            rootfs_dir = self.get_rootfs_dir(image_ref)
            layer_dir = os.path.join(os.path.dirname(rootfs_dir), "layer-0")
            os.makedirs(layer_dir, exist_ok=True)

        return ocilayer_pb2.PrepareImageResponse(
            image_digest="sha256:" + ("a" * 64),
            config_json=json.dumps(cfg).encode("utf-8"),
            layer_dirs=[layer_dir],
            provider_name=self._provider_name,
            rootfs_dir=rootfs_dir,
        )

    def ReleaseImage(self, request, context):
        image_ref = request.image_ref
        with self._lock:
            self.release_calls.append(image_ref)
            cur = self.ref_counts.get(image_ref, 0)
            self.ref_counts[image_ref] = max(0, cur - 1)
        return ocilayer_pb2.ReleaseImageResponse()


@pytest.fixture
def oci_layer_server(tmp_path, monkeypatch):
    sock_path = str(tmp_path / "ocilayer.sock")
    servicer = FakeOCILayerServicer(str(tmp_path / "streamed_mounts"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    ocilayer_pb2_grpc.add_OCILayerServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"unix://{sock_path}")
    server.start()
    monkeypatch.setenv("RAY_SANDBOX_OCI_LAYER_SOCKET", sock_path)
    yield servicer
    server.stop(grace=0)


def test_streamed_pull_metadata_spec_and_release(tmp_path, oci_layer_server):
    images_dir = str(tmp_path / "images")
    mgr = ImageManager(images_dir=images_dir)
    image_ref = "my-registry.example.com/team/agent:v1"

    with patch(
        "ray.experimental.sandbox.image_manager.pull_and_extract_container_image"
    ) as mock_fallback:
        image_dir = mgr.pull_image(image_ref, instance_id="sbx-1")
        mock_fallback.assert_not_called()

    assert mgr.is_image_extracted(image_ref)
    streamed_path = os.path.join(image_dir, STREAMED_MARKER)
    assert os.path.isfile(streamed_path)
    with open(streamed_path, "r", encoding="utf-8") as f:
        streamed_meta = json.load(f)

    expected_rootfs = oci_layer_server.get_rootfs_dir(image_ref)
    assert streamed_meta["rootfs_dir"] == expected_rootfs
    assert streamed_meta["provider"] == "fake-soci"
    assert streamed_meta["image_digest"] == "sha256:" + ("a" * 64)

    # Verify WorkingDir and Env parsed from Go v1.Config JSON
    assert mgr.get_workdir(image_ref) == "/workspace/agent"
    assert mgr.get_envs(image_ref) == [
        "PATH=/usr/local/bin:/usr/bin",
        "STREAMED_VAR=1",
    ]

    # Verify create_oci_spec uses streamed rootfs_dir with bind + memory overlay
    spec = mgr.create_oci_spec(
        image=image_ref,
        container_cwd="/workspace/agent",
        readonly=False,
        disk="512Mi",
        base_spec=_minimal_base_spec(),
    )
    assert spec["root"]["path"] == expected_rootfs
    assert spec["root"]["readonly"] is False
    annotations = spec.get("annotations", {})
    assert annotations.get("dev.gvisor.spec.rootfs.source") == expected_rootfs
    assert annotations.get("dev.gvisor.spec.rootfs.type") == "bind"
    assert annotations.get("dev.gvisor.spec.rootfs.overlay") == "memory"
    assert annotations.get("dev.gvisor.spec.rootfs.options") == f"size={512 * 1024**2}"

    # Verify create_oci_spec did not increment ref_count a second time
    assert oci_layer_server.ref_counts[image_ref] == 1
    assert len(oci_layer_server.prepare_calls) == 1

    # Second sandbox instance increments ref_count to 2
    mgr.pull_image(image_ref, instance_id="sbx-2")
    assert oci_layer_server.ref_counts[image_ref] == 2

    # Releasing sbx-1 decrements ref_count to 1
    mgr.release_image(image_ref, instance_id="sbx-1")
    assert oci_layer_server.ref_counts[image_ref] == 1

    # Releasing sbx-2 decrements ref_count to 0
    mgr.release_image(image_ref, instance_id="sbx-2")
    assert oci_layer_server.ref_counts[image_ref] == 0


def test_streamed_prepull_claimed_by_sandbox_instance(tmp_path, oci_layer_server):
    """Verify SandboxRuntime.create() pre-pull (instance_id=None) followed by
    backend create_sandbox (instance_id='sbx-1') results in a single lease pin.
    """
    images_dir = str(tmp_path / "images")
    mgr = ImageManager(images_dir=images_dir)
    image_ref = "my-registry.example.com/team/agent:v2"

    mgr.pull_image(image_ref)
    assert oci_layer_server.ref_counts[image_ref] == 1

    mgr.pull_image(image_ref, instance_id="sbx-1")
    assert oci_layer_server.ref_counts[image_ref] == 1

    mgr.release_image(image_ref, instance_id="sbx-1")
    assert oci_layer_server.ref_counts[image_ref] == 0


def test_streamed_fallback_when_can_provide_false(tmp_path, oci_layer_server):
    images_dir = str(tmp_path / "images")
    mgr = ImageManager(images_dir=images_dir)
    image_ref = "docker.io/library/busybox:latest"
    oci_layer_server.can_provide_result = False

    with patch(
        "ray.experimental.sandbox.image_manager.pull_and_extract_container_image",
        return_value=os.path.join(images_dir, "busybox_latest"),
    ) as mock_fallback:
        res = mgr.pull_image(image_ref, instance_id="sbx-fallback-1")
        assert res == os.path.join(images_dir, "busybox_latest")
        mock_fallback.assert_called_once_with(
            image_ref,
            images_dir=images_dir,
            timeout_seconds=120.0,
            instance_id="sbx-fallback-1",
        )
    assert len(oci_layer_server.prepare_calls) == 0


def test_streamed_fallback_on_failed_precondition(tmp_path, oci_layer_server):
    images_dir = str(tmp_path / "images")
    mgr = ImageManager(images_dir=images_dir)
    image_ref = "docker.io/library/alpine:latest"
    oci_layer_server.fail_precondition_images.add(image_ref)

    with patch(
        "ray.experimental.sandbox.image_manager.pull_and_extract_container_image",
        return_value=os.path.join(images_dir, "alpine_latest"),
    ) as mock_fallback:
        res = mgr.pull_image(image_ref, instance_id="sbx-fallback-2")
        assert res == os.path.join(images_dir, "alpine_latest")
        mock_fallback.assert_called_once_with(
            image_ref,
            images_dir=images_dir,
            timeout_seconds=120.0,
            instance_id="sbx-fallback-2",
        )
    assert oci_layer_server.prepare_calls == [image_ref]

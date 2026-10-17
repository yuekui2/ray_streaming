import json
import logging
import os
from typing import Any, Dict, Optional, Tuple

try:
    import grpc
    from ray.experimental.sandbox._internal import ocilayer_pb2, ocilayer_pb2_grpc
except ImportError:  # pragma: no cover
    grpc = None  # type: ignore[assignment]
    ocilayer_pb2 = None  # type: ignore[assignment]
    ocilayer_pb2_grpc = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

ENV_OCI_LAYER_SOCKET = "RAY_SANDBOX_OCI_LAYER_SOCKET"
DEFAULT_OCI_LAYER_SOCKET = "/run/ate-image-streaming/ocilayer.sock"
DEFAULT_CAN_PROVIDE_TIMEOUT_SECONDS = 2.0
DEFAULT_RELEASE_TIMEOUT_SECONDS = 10.0


class OCILayerError(RuntimeError):
    """Base error raised by OCILayerService client operations."""


class OCILayerNotSupportedError(OCILayerError):
    """Raised when the OCILayerService adapter declines an image (ErrNotSupported)."""


def get_oci_layer_socket_path(socket_path: Optional[str] = None) -> str:
    """Return the configured OCILayerService UNIX domain socket path."""
    if socket_path:
        return socket_path
    return os.environ.get(ENV_OCI_LAYER_SOCKET, DEFAULT_OCI_LAYER_SOCKET)


def normalize_image_config(raw_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Normalize OCI config JSON so top-level ``{"config": {...}}`` is always present.

    Go's ``OCILayerService`` serializes ``v1.Config`` directly (e.g.
    ``{"Env": [...], "WorkingDir": "..."}``), whereas ``ImageManager`` reads
    ``cfg.get("config", {}).get("WorkingDir")`` and ``cfg.get("config", {}).get("Env")``.
    """
    if not raw_config or not isinstance(raw_config, dict):
        return {"config": {}}
    if isinstance(raw_config.get("config"), dict):
        return raw_config
    return {"config": raw_config}


def can_provide(
    image_ref: str,
    timeout: float = DEFAULT_CAN_PROVIDE_TIMEOUT_SECONDS,
    socket_path: Optional[str] = None,
) -> bool:
    """Return True if the OCILayerService socket exists and can provide ``image_ref``."""
    sock = get_oci_layer_socket_path(socket_path)
    if not sock or not os.path.exists(sock) or grpc is None:
        return False

    target = f"unix://{sock}"
    try:
        with grpc.insecure_channel(target) as channel:
            stub = ocilayer_pb2_grpc.OCILayerServiceStub(channel)
            resp = stub.CanProvide(
                ocilayer_pb2.CanProvideRequest(image_ref=image_ref),
                timeout=timeout,
            )
            return bool(resp.can_provide)
    except Exception as e:
        logger.debug(
            "OCILayerService CanProvide failed for image %s on socket %s: %s",
            image_ref,
            sock,
            e,
        )
        return False


def prepare_image(
    image_ref: str,
    timeout: float = 120.0,
    socket_path: Optional[str] = None,
) -> Tuple[str, Dict[str, Any], str, str]:
    """Prepare a streamed rootfs for ``image_ref`` via OCILayerService.

    Returns:
        Tuple of ``(image_digest, config_dict, rootfs_dir, provider_name)``.

    Raises:
        OCILayerNotSupportedError: If the adapter returns FAILED_PRECONDITION.
        OCILayerError: If the RPC fails or returns an invalid rootfs_dir.
    """
    sock = get_oci_layer_socket_path(socket_path)
    if not sock or not os.path.exists(sock) or grpc is None:
        raise OCILayerNotSupportedError(
            f"OCILayerService socket does not exist or grpc is unavailable: {sock}"
        )

    target = f"unix://{sock}"
    try:
        with grpc.insecure_channel(target) as channel:
            stub = ocilayer_pb2_grpc.OCILayerServiceStub(channel)
            resp = stub.PrepareImage(
                ocilayer_pb2.PrepareImageRequest(image_ref=image_ref),
                timeout=timeout,
            )
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
            raise OCILayerNotSupportedError(
                f"OCILayerService declined image {image_ref}: {e.details()}"
            ) from e
        raise OCILayerError(
            f"OCILayerService PrepareImage failed for {image_ref}: {e}"
        ) from e

    raw_config: Dict[str, Any] = {}
    if resp.config_json:
        try:
            parsed = json.loads(resp.config_json.decode("utf-8"))
            if isinstance(parsed, dict):
                raw_config = parsed
        except Exception as e:
            logger.warning(
                "Failed to decode config_json from OCILayerService for %s: %s",
                image_ref,
                e,
            )
    config_dict = normalize_image_config(raw_config)

    rootfs_dir = resp.rootfs_dir
    if not rootfs_dir and len(resp.layer_dirs) == 1:
        candidate = os.path.join(resp.layer_dirs[0], "fs")
        rootfs_dir = candidate if os.path.exists(candidate) else resp.layer_dirs[0]

    if not rootfs_dir:
        raise OCILayerError(
            f"OCILayerService PrepareImage returned empty rootfs_dir for {image_ref}"
        )

    return (
        resp.image_digest,
        config_dict,
        rootfs_dir,
        resp.provider_name,
    )


def release_image(
    image_ref: str,
    bypass_ttl: bool = False,
    timeout: float = DEFAULT_RELEASE_TIMEOUT_SECONDS,
    socket_path: Optional[str] = None,
) -> None:
    """Decrement the active lease reference count for ``image_ref`` (best-effort)."""
    sock = get_oci_layer_socket_path(socket_path)
    if not sock or not os.path.exists(sock) or grpc is None:
        return

    target = f"unix://{sock}"
    try:
        with grpc.insecure_channel(target) as channel:
            stub = ocilayer_pb2_grpc.OCILayerServiceStub(channel)
            stub.ReleaseImage(
                ocilayer_pb2.ReleaseImageRequest(
                    image_ref=image_ref,
                    bypass_ttl=bypass_ttl,
                ),
                timeout=timeout,
            )
    except Exception as e:
        logger.debug(
            "OCILayerService ReleaseImage failed for %s on socket %s: %s",
            image_ref,
            sock,
            e,
        )

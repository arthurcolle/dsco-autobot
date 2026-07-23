from __future__ import annotations

import importlib
import logging
import os
from typing import Any, Dict, Mapping, Optional

try:
    # Warm import so Modal bundles the FastAPI app alongside this wrapper.
    import fastapi_app as _fastapi_app  # noqa: F401
except ModuleNotFoundError:
    _fastapi_app = None

try:
    import yaml  # type: ignore
except Exception as _e:  # pragma: no cover
    yaml = None

import modal

# ---------------------------
# Config loading
# ---------------------------


CONFIG_PATH = os.getenv("MODAL_CONFIG", "modal_config.yaml")


def load_config(path: str = CONFIG_PATH) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing Modal config file: {path}")
    if yaml is None:
        raise RuntimeError(
            "PyYAML is required to read modal_config.yaml. Add 'pyyaml' to image.pip in modal_config.yaml"
        )
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("modal_config.yaml must define a YAML mapping at the top level")
    return data  # type: ignore[return-value]


try:
    CFG = load_config()
except Exception:
    # In container or when config is intentionally omitted, fall back to defaults.
    CFG = {}


# ---------------------------
# Image building
# ---------------------------


def build_image(cfg: Mapping[str, Any]) -> modal.Image:
    image_cfg = cfg.get("image", {}) or {}
    base = (image_cfg.get("base") or "debian_slim").lower()

    # Choose base image. Extendable with more bases as needed.
    if base == "debian_slim":
        img = modal.Image.debian_slim()
    else:
        # Fallback to debian_slim if unknown
        img = modal.Image.debian_slim()

    apt = image_cfg.get("apt") or []
    if apt:
        img = img.apt_install(*apt)

    pip = image_cfg.get("pip") or []
    # Ensure fastapi runtime deps if not specified
    if not pip:
        pip = ["fastapi[standard]", "uvicorn", "pyyaml"]
    img = img.pip_install(*pip)

    return img


def _with_default_source_files(image: modal.Image) -> modal.Image:
    """Ensure the FastAPI app and config file are available in the container."""
    tracked: Dict[str, str] = {
        "fastapi_app.py": "/root/fastapi_app.py",
        "modal_config.yaml": "/root/modal_config.yaml",
        "conversation_api_dataclasses.py": "/root/conversation_api_dataclasses.py",
    }
    for local_path, remote_path in tracked.items():
        if os.path.exists(local_path):
            image = image.add_local_file(local_path, remote_path=remote_path, copy=False)
    return image


IMAGE = _with_default_source_files(build_image(CFG))


# ---------------------------
# Modal App + resources
# ---------------------------


APP_NAME = str(CFG.get("name") or "fastapi-on-modal")
app = modal.App(APP_NAME, image=IMAGE)


def _build_secrets(cfg: Mapping[str, Any]):
    secrets_cfg = cfg.get("secrets") or []
    secrets = []
    for s in secrets_cfg:
        if isinstance(s, str):
            secrets.append(modal.Secret.from_name(s))
        elif isinstance(s, Mapping) and s.get("name"):
            secrets.append(modal.Secret.from_name(str(s["name"])))
    return secrets


def _build_volumes(cfg: Mapping[str, Any]):
    volumes_cfg = cfg.get("volumes") or []
    vol_map = {}
    for v in volumes_cfg:
        if not isinstance(v, Mapping):
            continue
        name = str(v.get("name"))
        if not name:
            continue
        mount_path = str(v.get("mount_path") or f"/volumes/{name}")
        create = bool(v.get("create_if_missing", True))
        vol_map[mount_path] = modal.Volume.from_name(name, create_if_missing=create)
    return vol_map


def _coerce_label(value: Any) -> Optional[str]:
    if value is None:
        return None
    label = str(value).strip()
    return label or None


def _endpoint_label(cfg: Mapping[str, Any]) -> Optional[str]:
    label = _coerce_label(os.getenv("MODAL_WEB_LABEL"))
    if label:
        return label
    domain_cfg = cfg.get("domain") if isinstance(cfg.get("domain"), Mapping) else {}
    if isinstance(domain_cfg, Mapping):
        label = _coerce_label(domain_cfg.get("label"))
        if label:
            return label
    return _coerce_label(cfg.get("label"))


def _endpoint_custom_domains(cfg: Mapping[str, Any]) -> Optional[list[str]]:
    domain_cfg = cfg.get("domain") if isinstance(cfg.get("domain"), Mapping) else {}
    if not isinstance(domain_cfg, Mapping):
        return None

    raw_domains = domain_cfg.get("custom_domains")
    if raw_domains is None:
        raw_domains = domain_cfg.get("custom_domain")

    if raw_domains is None:
        return None

    if isinstance(raw_domains, str):
        candidates = [raw_domains]
    else:
        try:
            candidates = list(raw_domains)
        except TypeError:
            candidates = []

    cleaned = [_coerce_label(item) for item in candidates]
    result = [item for item in cleaned if item]
    return result or None


def _function_kwargs(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    fcfg = cfg.get("function") or {}
    kwargs: Dict[str, Any] = {}
    if "timeout" in fcfg:
        kwargs["timeout"] = int(fcfg["timeout"])  # seconds
    if "cpu" in fcfg:
        kwargs["cpu"] = fcfg["cpu"]
    if "memory" in fcfg:
        kwargs["memory"] = fcfg["memory"]
    if "gpu" in fcfg and fcfg["gpu"]:
        kwargs["gpu"] = fcfg["gpu"]
    if "min_containers" in fcfg:
        kwargs["min_containers"] = int(fcfg["min_containers"]) or 0
    elif "keep_warm" in fcfg:
        # Support legacy keep_warm parameter
        kwargs["min_containers"] = int(fcfg["keep_warm"]) or 0
    # Base environment from config
    env_cfg: Dict[str, str] = dict(cfg.get("env") or {})
    # Propagate critical settings so the container doesn't need the YAML file
    env_cfg.setdefault("MODAL_IMPORT_PATH", str(cfg.get("import_path") or "fastapi_app:app"))
    auth_cfg = cfg.get("auth") or {}
    if auth_cfg:
        if auth_cfg.get("enabled") is True:
            env_cfg.setdefault("MODAL_AUTH_ENABLED", "1")
        if auth_cfg.get("bearer_env_var"):
            env_cfg.setdefault("MODAL_AUTH_ENV_VAR", str(auth_cfg.get("bearer_env_var")))
    if env_cfg:
        kwargs["env"] = env_cfg
    vols = _build_volumes(cfg)
    if vols:
        kwargs["volumes"] = vols
    secrets = _build_secrets(cfg)
    if secrets:
        kwargs["secrets"] = secrets
    return kwargs


CONCURRENCY = int((CFG.get("function") or {}).get("concurrency", 100))
ENDPOINT_LABEL = _endpoint_label(CFG)
ENDPOINT_CUSTOM_DOMAINS = _endpoint_custom_domains(CFG)


# ---------------------------
# App import and optional wrappers
# ---------------------------


def import_fastapi_app(import_path: str):
    """Import a FastAPI app (or factory) from module:attr.

    - If attr is a FastAPI instance (or any ASGI app), return it.
    - If attr is callable, call it without args and expect a FastAPI/ASGI app.
    """

    module_name, _, attr_name = import_path.partition(":")
    if not module_name:
        raise ValueError("import_path must be in the form 'module:attr'")
    if not attr_name:
        # default to 'app' when only module is given
        attr_name = "app"

    module = importlib.import_module(module_name)
    target = getattr(module, attr_name)
    # If it's a factory, call it (no args)
    if callable(target):
        try:
            instance = target()
            target = instance
        except TypeError:
            # Could be an ASGI app (callable) itself; just use as-is
            pass
    return target


class BearerAuthMiddleware:
    """Minimal ASGI middleware to enforce a static Bearer token.

    Reads expected token from the provided string, typically set via secrets/env.
    Enable via modal_config.yaml -> auth.enabled: true.
    """

    def __init__(self, app: Any, token: str) -> None:
        self.app = app
        self.token = token

    async def __call__(self, scope, receive, send):  # type: ignore[override]
        if scope.get("type") != "http":
            return await self.app(scope, receive, send)

        headers = {k.decode("latin1"): v.decode("latin1") for k, v in (scope.get("headers") or [])}
        auth = headers.get("authorization") or ""
        if not auth.startswith("Bearer "):
            return await _unauthorized(send)
        provided = auth[len("Bearer ") :].strip()
        if provided != self.token:
            return await _unauthorized(send)
        return await self.app(scope, receive, send)


async def _unauthorized(send):  # type: ignore[override]
    body = b'{"detail":"Unauthorized"}'
    await send({
        "type": "http.response.start",
        "status": 401,
        "headers": [
            (b"content-type", b"application/json"),
            (b"www-authenticate", b"Bearer"),
        ],
    })
    await send({"type": "http.response.body", "body": body})


def maybe_wrap_auth(asgi_app: Any, cfg: Mapping[str, Any]) -> Any:
    # Environment overrides win, so we can run without local YAML in container
    env_enabled = os.getenv("MODAL_AUTH_ENABLED")
    enabled_from_env = env_enabled in ("1", "true", "True")
    auth_cfg = cfg.get("auth") or {}
    enabled_from_cfg = bool(auth_cfg.get("enabled"))
    if not (enabled_from_env or enabled_from_cfg):
        return asgi_app

    env_var = os.getenv("MODAL_AUTH_ENV_VAR") or str(auth_cfg.get("bearer_env_var") or "AUTH_TOKEN")
    token = os.getenv(env_var)
    if not token:
        logging.getLogger(__name__).warning(
            "Auth enabled but %s not set; requests will be rejected with 401", env_var
        )
        token = "__missing__"
    return BearerAuthMiddleware(asgi_app, token)


# ---------------------------
# Modal ASGI wrapper
# ---------------------------


# Modal >=1.2 encourages relying on Image.add_local_* instead of modal.Mount,
# so the files are wired into IMAGE via _with_default_source_files above.
@app.function(**_function_kwargs(CFG))
@modal.concurrent(max_inputs=CONCURRENCY)
@modal.asgi_app(label=ENDPOINT_LABEL, custom_domains=ENDPOINT_CUSTOM_DOMAINS)
def modal_fastapi_app():
    import_path = os.getenv("MODAL_IMPORT_PATH") or str(CFG.get("import_path") or "fastapi_app:app")
    asgi = import_fastapi_app(import_path)
    asgi = maybe_wrap_auth(asgi, CFG)
    return asgi


if __name__ == "__main__":  # Convenience: show quick usage hints
    print("Use one of the following:")
    print("  modal serve modal_wrapper.py     # live-reload local dev")
    print("  modal deploy modal_wrapper.py    # deploy persistent endpoint")
    print()
    print(f"App name: {APP_NAME}")
    if CFG.get("domain", {}).get("custom_domain"):
        print("Note: configure custom domains in Modal dashboard/CLI during deploy.")

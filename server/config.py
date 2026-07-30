"""Validated operator configuration; secrets are never part of its repr."""

from __future__ import annotations

import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from ipaddress import ip_address
from math import isfinite
from pathlib import Path
from urllib.parse import urlparse


class ConfigError(ValueError):
    """Raised when an operator setting is invalid or not allowlisted."""


def _is_loopback_host(value: str) -> bool:
    host = value.strip().strip("[]")
    if host.lower() == "localhost":
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


def _models(value: Mapping[str, str]) -> dict[str, str]:
    result = dict(value)
    if not result or any(not label or not model for label, model in result.items()):
        raise ConfigError("model policies must contain non-empty labels and model IDs")
    return result


@dataclass(frozen=True)
class Config:
    openai_api_key: str | None = field(default=None, repr=False)
    openai_api_key_env: str = "OPENAI_API_KEY"
    deepgram_api_key: str | None = field(default=None, repr=False)
    cartesia_api_key: str | None = field(default=None, repr=False)
    cartesia_voice_id: str | None = field(default=None, repr=False)
    router_model_policy: Mapping[str, str] = field(default_factory=lambda: {"fast": "gpt-5-mini"})
    worker_model_policy: Mapping[str, str] = field(default_factory=lambda: {"deep": "gpt-5"})
    max_work_items_per_turn: int = 2
    multi_intent_wait_timeout_ms: int = 10_000
    foreground_search_timeout_seconds: float = 15.0
    router_timeout_seconds: float = 12.0
    provider_timeout_seconds: float = 75.0
    shutdown_grace_seconds: float = 2.0
    max_citations: int = 12
    pending_dialogue_timeout_seconds: float = 30.0
    stt_service: str = "websocket"
    stt_provider: str = "local"
    stt_model: str = "nova-3-general"
    stt_language: str = "en"
    stt_endpoint: tuple[str, str] | None = None
    smart_turn_timeout_seconds: float = 5.0
    smart_turn_complete_grace_seconds: float = 1.5
    speech_start_timeout_seconds: float = 10.0
    speech_transport_grace_seconds: float = 1.0
    tts_endpoint: tuple[str, str] | None = None
    tts_provider: str = "local"
    tts_model: str = "sonic-3.5"
    tts_voice_id: str = "azelma"
    bind_host: str = "127.0.0.1"
    bind_port: int = 7860
    known_client_url: str = "http://127.0.0.1:7860"

    def __post_init__(self) -> None:
        if self.max_work_items_per_turn not in (2, 3, 4):
            raise ConfigError("max_work_items_per_turn must be 2, 3, or 4")
        if self.multi_intent_wait_timeout_ms <= 0:
            raise ConfigError("multi_intent_wait_timeout_ms must be positive")
        for name in (
            "foreground_search_timeout_seconds",
            "router_timeout_seconds",
            "provider_timeout_seconds",
            "shutdown_grace_seconds",
            "speech_start_timeout_seconds",
            "speech_transport_grace_seconds",
        ):
            value = getattr(self, name)
            if not isfinite(value) or value <= 0:
                raise ConfigError(f"{name} must be finite and positive")
        if self.provider_timeout_seconds <= self.foreground_search_timeout_seconds:
            raise ConfigError(
                "provider_timeout_seconds must exceed foreground_search_timeout_seconds"
            )
        if self.router_timeout_seconds > self.provider_timeout_seconds:
            raise ConfigError("router_timeout_seconds must not exceed provider_timeout_seconds")
        if not 1 <= self.max_citations <= 50:
            raise ConfigError("max_citations must be between 1 and 50")
        if (
            not isfinite(self.pending_dialogue_timeout_seconds)
            or self.pending_dialogue_timeout_seconds <= 0
        ):
            raise ConfigError("pending_dialogue_timeout_seconds must be finite and positive")
        if not self.bind_host.strip():
            raise ConfigError("bind_host must not be empty")
        if not _is_loopback_host(self.bind_host):
            raise ConfigError(
                "bind_host must be loopback; remote deployment requires an authenticated transport"
            )
        if not 1 <= self.bind_port <= 65_535:
            raise ConfigError("bind_port must be between 1 and 65535")
        parsed_client_url = urlparse(self.known_client_url)
        if parsed_client_url.scheme not in {"http", "https"} or not parsed_client_url.netloc:
            raise ConfigError("known_client_url must be an absolute http(s) URL")
        if not self.openai_api_key_env.isidentifier() or not self.openai_api_key_env.isupper():
            raise ConfigError("openai_api_key_env must be an uppercase environment variable name")
        if self.stt_service != "websocket":
            raise ConfigError("stt_service must be websocket")
        if self.stt_provider not in {"local", "deepgram"}:
            raise ConfigError("stt_provider must be local or deepgram")
        if not self.stt_model.strip():
            raise ConfigError("stt_model must not be empty")
        if self.stt_language != "auto" and not re.fullmatch(
            r"[A-Za-z]{2,3}(?:[-_][A-Za-z]{2,4})?", self.stt_language
        ):
            raise ConfigError("stt_language must be auto or an ISO language code")
        if not 0 < self.smart_turn_timeout_seconds <= 60:
            raise ConfigError("smart_turn_timeout_seconds must be between 0 and 60")
        if not 0 < self.smart_turn_complete_grace_seconds <= 10:
            raise ConfigError("smart_turn_complete_grace_seconds must be between 0 and 10")
        if self.tts_provider not in {"local", "cartesia"}:
            raise ConfigError("tts_provider must be local or cartesia")
        if not self.tts_model.strip():
            raise ConfigError("tts_model must not be empty")
        if not self.tts_voice_id.strip():
            raise ConfigError("tts_voice_id must not be empty")
        object.__setattr__(self, "router_model_policy", _models(self.router_model_policy))
        object.__setattr__(self, "worker_model_policy", _models(self.worker_model_policy))

    def resolve_router_model(self, policy_label: str) -> str:
        try:
            return self.router_model_policy[policy_label]
        except KeyError as exc:
            raise ConfigError(f"unknown router model policy: {policy_label}") from exc

    def resolve_worker_model(self, policy_label: str) -> str:
        try:
            return self.worker_model_policy[policy_label]
        except KeyError as exc:
            raise ConfigError(f"unknown worker model policy: {policy_label}") from exc

    def with_discovered_endpoint(self, service: str, transport: str, address: str) -> Config:
        if service not in {"stt", "tts"}:
            raise ConfigError(f"unknown service: {service}")
        return replace(self, **{f"{service}_endpoint": (transport, address)})


def load_config(
    *,
    env: Mapping[str, str] | None = None,
    env_file: Mapping[str, str] | str | Path | None = None,
    config_file: str | Path | None = None,
) -> Config:
    """Load TOML defaults, then env-file values, then process environment."""
    values: dict[str, object] = {}
    toml_values: dict[str, object] = {}
    if config_file is None and env is None:
        config_file = Path(__file__).resolve().parents[1] / "config.toml"
    if config_file:
        path = Path(config_file).expanduser()
        if path.exists():
            try:
                with path.open("rb") as handle:
                    toml_values = tomllib.load(handle)
            except tomllib.TOMLDecodeError as exc:
                raise ConfigError(f"invalid TOML config: {path}") from exc
    _load_toml_values(values, toml_values)
    if env_file:
        if isinstance(env_file, (str, Path)):
            for line in Path(env_file).read_text().splitlines():
                if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    values[key.strip()] = value.strip().strip("\"'")
        else:
            values.update(env_file)
    values.update(os.environ if env is None else env)
    kwargs: dict[str, object] = {}
    key_name = values.get("WEBSEARCH_OPENAI_API_KEY_ENV", "OPENAI_API_KEY")
    if key := values.get("WEBSEARCH_OPENAI_API_KEY") or values.get(key_name):
        kwargs["openai_api_key"] = key
    if key := values.get("WEBSEARCH_DEEPGRAM_API_KEY") or values.get("DEEPGRAM_API_KEY"):
        kwargs["deepgram_api_key"] = key
    if key := values.get("WEBSEARCH_CARTESIA_API_KEY") or values.get("CARTESIA_API_KEY"):
        kwargs["cartesia_api_key"] = key
    if voice := values.get("WEBSEARCH_CARTESIA_VOICE_ID") or values.get("CARTESIA_VOICE_ID"):
        kwargs["cartesia_voice_id"] = voice
    if raw := values.get("WEBSEARCH_MAX_WORK_ITEMS_PER_TURN"):
        kwargs["max_work_items_per_turn"] = int(raw)
    if raw := values.get("WEBSEARCH_MULTI_INTENT_WAIT_TIMEOUT_MS"):
        kwargs["multi_intent_wait_timeout_ms"] = int(raw)
    for env_name, field_name in (
        ("WEBSEARCH_FOREGROUND_SEARCH_TIMEOUT_SECONDS", "foreground_search_timeout_seconds"),
        ("WEBSEARCH_ROUTER_TIMEOUT_SECONDS", "router_timeout_seconds"),
        ("WEBSEARCH_PROVIDER_TIMEOUT_SECONDS", "provider_timeout_seconds"),
        ("WEBSEARCH_SHUTDOWN_GRACE_SECONDS", "shutdown_grace_seconds"),
    ):
        if env_name in values:
            try:
                kwargs[field_name] = float(values[env_name])
            except (TypeError, ValueError) as exc:
                raise ConfigError(f"{env_name} must be a number") from exc
    if raw := values.get("WEBSEARCH_MAX_CITATIONS"):
        try:
            kwargs["max_citations"] = int(raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError("WEBSEARCH_MAX_CITATIONS must be an integer") from exc
    if "WEBSEARCH_PENDING_DIALOGUE_TIMEOUT_SECONDS" in values:
        raw = values["WEBSEARCH_PENDING_DIALOGUE_TIMEOUT_SECONDS"]
        try:
            kwargs["pending_dialogue_timeout_seconds"] = float(raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                "WEBSEARCH_PENDING_DIALOGUE_TIMEOUT_SECONDS must be a number"
            ) from exc
    if raw := values.get("WEBSEARCH_STT_SERVICE"):
        kwargs["stt_service"] = str(raw)
    if raw := values.get("WEBSEARCH_STT_PROVIDER"):
        kwargs["stt_provider"] = str(raw)
    if raw := values.get("WEBSEARCH_STT_MODEL"):
        kwargs["stt_model"] = str(raw)
    if raw := values.get("WEBSEARCH_STT_LANGUAGE"):
        kwargs["stt_language"] = str(raw)
    if raw := values.get("WEBSEARCH_SMART_TURN_TIMEOUT_SECONDS"):
        try:
            kwargs["smart_turn_timeout_seconds"] = float(raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError("WEBSEARCH_SMART_TURN_TIMEOUT_SECONDS must be a number") from exc
    if raw := values.get("WEBSEARCH_SMART_TURN_COMPLETE_GRACE_SECONDS"):
        try:
            kwargs["smart_turn_complete_grace_seconds"] = float(raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                "WEBSEARCH_SMART_TURN_COMPLETE_GRACE_SECONDS must be a number"
            ) from exc
    if (raw := values.get("WEBSEARCH_SPEECH_START_TIMEOUT_SECONDS")) is not None:
        try:
            kwargs["speech_start_timeout_seconds"] = float(raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError("WEBSEARCH_SPEECH_START_TIMEOUT_SECONDS must be a number") from exc
    if (raw := values.get("WEBSEARCH_SPEECH_TRANSPORT_GRACE_SECONDS")) is not None:
        try:
            kwargs["speech_transport_grace_seconds"] = float(raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError("WEBSEARCH_SPEECH_TRANSPORT_GRACE_SECONDS must be a number") from exc
    if raw := values.get("WEBSEARCH_TTS_VOICE_ID"):
        kwargs["tts_voice_id"] = str(raw)
    if raw := values.get("WEBSEARCH_TTS_PROVIDER"):
        kwargs["tts_provider"] = str(raw)
    if raw := values.get("WEBSEARCH_TTS_MODEL"):
        kwargs["tts_model"] = str(raw)
    if "WEBSEARCH_BIND_HOST" in values:
        kwargs["bind_host"] = values["WEBSEARCH_BIND_HOST"]
    if "WEBSEARCH_BIND_PORT" in values:
        raw = values["WEBSEARCH_BIND_PORT"]
        try:
            kwargs["bind_port"] = int(raw)
        except ValueError as exc:
            raise ConfigError("WEBSEARCH_BIND_PORT must be an integer") from exc
    if "WEBSEARCH_KNOWN_CLIENT_URL" in values:
        kwargs["known_client_url"] = values["WEBSEARCH_KNOWN_CLIENT_URL"]
    if raw := values.get("WEBSEARCH_OPENAI_API_KEY_ENV"):
        kwargs["openai_api_key_env"] = raw
    if raw := values.get("WEBSEARCH_ROUTER_MODEL"):
        kwargs["router_model_policy"] = {"fast": str(raw)}
    if raw := values.get("WEBSEARCH_WORKER_MODEL"):
        kwargs["worker_model_policy"] = {"deep": str(raw)}
    stt_endpoint = values.get("WEBSEARCH_STT_ENDPOINT")
    if stt_endpoint:
        kwargs["stt_endpoint"] = parse_endpoint(str(stt_endpoint))
    elif values.get("WEBSEARCH_STT_WS_SOCKET"):
        kwargs["stt_endpoint"] = ("uds", _expand_socket(str(values["WEBSEARCH_STT_WS_SOCKET"])))
    tts_endpoint = values.get("WEBSEARCH_TTS_ENDPOINT")
    if tts_endpoint:
        kwargs["tts_endpoint"] = parse_endpoint(str(tts_endpoint))
    else:
        tts_uri = values.get("WEBSEARCH_TTS_WS_URI")
        tts_socket = values.get("WEBSEARCH_TTS_WS_SOCKET")
        tts_host = values.get("WEBSEARCH_TTS_WS_HOST")
        tts_port = values.get("WEBSEARCH_TTS_WS_PORT")
        if tts_uri:
            kwargs["tts_endpoint"] = parse_endpoint(str(tts_uri))
        elif tts_socket:
            kwargs["tts_endpoint"] = ("uds", _expand_socket(str(tts_socket)))
        elif tts_host and tts_port:
            try:
                port = int(str(tts_port))
            except ValueError as exc:
                raise ConfigError("WEBSEARCH_TTS_WS_PORT must be an integer") from exc
            if not 1 <= port <= 65_535:
                raise ConfigError("WEBSEARCH_TTS_WS_PORT must be between 1 and 65535")
            kwargs["tts_endpoint"] = ("ws", f"{tts_host}:{port}")
    return Config(**kwargs)


def _load_toml_values(values: dict[str, object], document: Mapping[str, object]) -> None:
    """Map the non-secret local runtime TOML surface into config values."""
    stt = document.get("stt", {})
    tts = document.get("tts", {})
    turn = document.get("turn", {})
    models = document.get("models", {})
    if not all(isinstance(section, Mapping) for section in (stt, tts, turn, models)):
        raise ConfigError("[stt], [tts], [turn], and [models] config sections must be tables")
    if "stt_service" in stt:
        values["WEBSEARCH_STT_SERVICE"] = stt["stt_service"]
    if "provider" in stt:
        values["WEBSEARCH_STT_PROVIDER"] = stt["provider"]
    if "model" in stt:
        values["WEBSEARCH_STT_MODEL"] = stt["model"]
    if "stt_language" in stt:
        values["WEBSEARCH_STT_LANGUAGE"] = stt["stt_language"]
    if "stt_ws_socket" in stt:
        values["WEBSEARCH_STT_WS_SOCKET"] = stt["stt_ws_socket"]
    if "smart_turn_timeout_seconds" in turn:
        values["WEBSEARCH_SMART_TURN_TIMEOUT_SECONDS"] = turn["smart_turn_timeout_seconds"]
    if "smart_turn_complete_grace_seconds" in turn:
        values["WEBSEARCH_SMART_TURN_COMPLETE_GRACE_SECONDS"] = turn[
            "smart_turn_complete_grace_seconds"
        ]
    if "speech_start_timeout_seconds" in turn:
        values["WEBSEARCH_SPEECH_START_TIMEOUT_SECONDS"] = turn["speech_start_timeout_seconds"]
    if "speech_transport_grace_seconds" in turn:
        values["WEBSEARCH_SPEECH_TRANSPORT_GRACE_SECONDS"] = turn["speech_transport_grace_seconds"]
    if "pending_dialogue_timeout_seconds" in turn:
        values["WEBSEARCH_PENDING_DIALOGUE_TIMEOUT_SECONDS"] = turn[
            "pending_dialogue_timeout_seconds"
        ]
    for key in (
        "foreground_search_timeout_seconds",
        "router_timeout_seconds",
        "provider_timeout_seconds",
        "shutdown_grace_seconds",
        "max_citations",
    ):
        if key in turn:
            values[f"WEBSEARCH_{key.upper()}"] = turn[key]
    if "router_model" in models:
        values["WEBSEARCH_ROUTER_MODEL"] = models["router_model"]
    if "worker_model" in models:
        values["WEBSEARCH_WORKER_MODEL"] = models["worker_model"]
    for key in ("tts_ws_uri", "tts_ws_socket", "tts_ws_host", "tts_ws_port"):
        if key in tts:
            values[f"WEBSEARCH_{key.upper()}"] = tts[key]
    if "provider" in tts:
        values["WEBSEARCH_TTS_PROVIDER"] = tts["provider"]
    if "model" in tts:
        values["WEBSEARCH_TTS_MODEL"] = tts["model"]
    if "voice_id" in tts:
        values["WEBSEARCH_TTS_VOICE_ID"] = tts["voice_id"]


def parse_endpoint(value: str) -> tuple[str, str]:
    """Parse an operator endpoint without assuming UDS or TCP in advance."""
    if "://" not in value:
        raise ConfigError("service endpoints must use transport://address form")
    transport, address = value.split("://", 1)
    if transport not in {"uds", "tcp", "ws", "wss"} or not address:
        raise ConfigError("unsupported or empty service endpoint")
    if transport == "uds":
        address = _expand_socket(address)
    return transport, address


def _expand_socket(value: str) -> str:
    address = os.path.expanduser(value).strip()
    if not address:
        raise ConfigError("service socket path must not be empty")
    return address

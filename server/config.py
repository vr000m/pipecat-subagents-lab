"""Validated operator configuration; secrets are never part of its repr."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _package_version
from ipaddress import ip_address
from math import isfinite
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

_FALLBACK_RELEASE_VERSION = "0.1.3"


def _installed_release_version() -> str:
    """The packaged project version, or a pinned literal when unavailable.

    A non-installed checkout (no distribution metadata) still has to boot, so
    the lookup is best-effort rather than fatal. Keeping this derived from
    package metadata avoids a second hand-maintained copy of the version that
    can silently drift from ``pyproject.toml``.
    """
    try:
        return _package_version("pipecat-subagents-lab")
    except PackageNotFoundError:
        return _FALLBACK_RELEASE_VERSION


_DEFAULT_RELEASE_VERSION = _installed_release_version()


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
    enable_early_ack: bool = True
    enable_background_status: bool = True
    enable_autoplay_policy: bool = True
    early_ack_text: str = "One moment while I look into that."
    promotion_manifest_path: str = "docs/benchmarks/v0.1.3-promotion-manifest.json"
    phase4c_artifact_path: str | None = None
    release_version: str = _DEFAULT_RELEASE_VERSION
    source_commit: str | None = None
    source_tree_hash: str | None = None
    deployed_at_utc: str | None = None

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
        if not self.early_ack_text.strip():
            raise ConfigError("early_ack_text must not be empty")
        if not self.promotion_manifest_path.strip():
            raise ConfigError("promotion_manifest_path must not be empty")
        if not self.release_version.strip():
            raise ConfigError("release_version must not be empty")
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


@dataclass(frozen=True)
class FeaturePolicy:
    """Immutable v0.1.3 kill-switch snapshot, resolved once from a `Config`.

    `SessionHost` owns/injects one instance per host; connection pipelines,
    observers, and policy evaluators consume this same object rather than
    re-reading `Config` independently.
    """

    enable_early_ack: bool = True
    enable_background_status: bool = True
    enable_autoplay_policy: bool = True

    @classmethod
    def from_config(cls, config: Config) -> FeaturePolicy:
        """Project the three kill switches out of a `Config`.

        Both `Config` and `FeaturePolicy` are frozen dataclasses with
        structural equality, so this resolves unconditionally on every call
        and callers compare policies with ``==``, never ``is``. Two
        field-equal `Config` objects therefore yield equal policies.
        """
        return cls(
            enable_early_ack=config.enable_early_ack,
            enable_background_status=config.enable_background_status,
            enable_autoplay_policy=config.enable_autoplay_policy,
        )


def feature_policy_fingerprint(policy: FeaturePolicy) -> str:
    """Deterministic identity for one frozen `FeaturePolicy` snapshot.

    Used to bind a promotion manifest to the exact kill-switch state it was
    generated against; a manifest fingerprinted against a different policy
    combination is stale and `load_promotion_manifest` rejects it.
    """
    canonical = json.dumps(
        {
            "enable_early_ack": policy.enable_early_ack,
            "enable_background_status": policy.enable_background_status,
            "enable_autoplay_policy": policy.enable_autoplay_policy,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PromotionManifest:
    """The immutable, fail-closed verdict `load_promotion_manifest` hands to `SessionHost`.

    `promotion_eligible=True` is the *only* state a policy evaluator may
    treat as data-driven-tuning-enabled. Every other combination -- missing
    file, malformed JSON, schema mismatch, wrong/incomplete manifest phase,
    stale identity binding, or an explicit `promotion_eligible=false` in the
    manifest itself -- resolves to `promotion_eligible=False` with `reason`
    set to the precise cause. This loader never raises for a missing or
    malformed manifest; runtime boot is fail-closed, not fail-fast.
    """

    promotion_eligible: bool
    reason: str | None = None
    manifest_phase: str | None = None
    source_commit: str | None = None
    source_tree_hash: str | None = None
    release_version: str | None = None
    feature_policy_fingerprint: str | None = None
    generated_at_utc: str | None = None


_MANIFEST_REQUIRED_FIELDS = frozenset(
    {
        "manifest_phase",
        "promotion_eligible",
        "reason",
        "schema_hash",
        "source_commit",
        "source_tree_hash",
        "release_version",
        "feature_policy_fingerprint",
        "deployed_at_utc",
        "generated_at_utc",
        "inputs",
    }
)


# Every phase a `final` manifest must carry an `inputs` binding for.
_MANIFEST_REQUIRED_FINAL_INPUTS = frozenset({"phase0", "phase1", "phase2", "phase3"})


_HEX_HASH_PATTERN = re.compile(r"[0-9a-fA-F]{64}")

# Manifest keys whose value must be a JSON string. `reason` is deliberately
# excluded: it is required to be present but is null on an eligible manifest.
_MANIFEST_STRING_FIELDS = (
    "manifest_phase",
    "schema_hash",
    "source_commit",
    "source_tree_hash",
    "release_version",
    "feature_policy_fingerprint",
    "deployed_at_utc",
    "generated_at_utc",
)


def _unavailable(reason: str) -> PromotionManifest:
    return PromotionManifest(promotion_eligible=False, reason=reason)


def _is_hex_hash(value: object) -> bool:
    return isinstance(value, str) and _HEX_HASH_PATTERN.fullmatch(value) is not None


_REPO_ROOT = Path(__file__).resolve().parents[1]

_EVIDENCE_SCHEMA_PATH = _REPO_ROOT / "shared/schemas/v013-evidence.json"

# A manifest-declared evidence input is attacker-steerable (it lives inside the
# artifact under scrutiny), so it is capped well above any real evidence file
# to bound the read even once path confinement has already ruled out traversal.
_MAX_EVIDENCE_INPUT_BYTES = 8 * 1024 * 1024


def _regular_file_within_evidence_size_cap(path: Path) -> bool:
    """True iff `path` names a regular file at or under `_MAX_EVIDENCE_INPUT_BYTES`.

    A single `os.stat()`/`Path.stat()` call backs both checks: `Path.is_file()`
    is itself a `stat()` call, so testing it and `Path.stat().st_size`
    separately would double the syscall for every evidence-file read this
    gates. `stat.S_ISREG` reads the file-type bits off that one call's result
    instead.
    """
    from stat import S_ISREG

    try:
        st = path.stat()
    except OSError:
        return False
    return S_ISREG(st.st_mode) and st.st_size <= _MAX_EVIDENCE_INPUT_BYTES


def _resolve_confined_evidence_path(raw_path: str) -> Path | None:
    """Resolve a manifest-declared evidence path, confined to the repo tree
    and bounded in size.

    `raw_path` comes from `manifest["inputs"][phase]["path"]`, a value the
    artifact under validation declares about itself -- not an operator
    setting. Accepting it as a runtime read target without containment would
    be an attacker-steerable arbitrary-file-read primitive (absolute paths,
    `..` traversal, devices, FIFOs). Returns `None` when the path is absolute,
    escapes the repo root after resolution, does not name a regular file, or
    exceeds `_MAX_EVIDENCE_INPUT_BYTES` -- the size cap lives here, not at the
    call site, so every caller inherits both the confinement and the read-size
    bound together rather than having to remember to apply the cap itself.
    """
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return None
    resolved = (_REPO_ROOT / candidate).resolve()
    if not resolved.is_relative_to(_REPO_ROOT):
        return None
    if not _regular_file_within_evidence_size_cap(resolved):
        return None
    return resolved


def _schema_hash_matches(declared: str) -> Literal["match", "mismatch", "unverifiable"]:
    """Recompute the evidence schema digest the manifest declares.

    The writer stamps `schema_hash` from `shared/schemas/v013-evidence.json`;
    accepting it as "some 64-hex string" let a forged manifest declare an
    arbitrary schema binding. The evidence schemas are deliberately excluded
    from the deployable runtime set, so a packaged install may genuinely not
    ship this file -- but "cannot verify" is not "verified", so it is reported
    as its own state rather than collapsed into a match, and the caller decides
    fail-closed.
    """
    try:
        data = _EVIDENCE_SCHEMA_PATH.read_bytes()
    except OSError:
        return "unverifiable"
    if hashlib.sha256(data).hexdigest() == declared:
        return "match"
    return "mismatch"


def _parse_utc_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp into an aware UTC instant.

    `datetime.fromisoformat` historically rejects a bare trailing ``Z``, and a
    naive timestamp cannot be compared against an aware one, so both are
    normalised here. Raises `ValueError` on anything unparseable; the caller's
    fail-closed boundary turns that into a display-only verdict.
    """
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def load_promotion_manifest(config: Config) -> PromotionManifest:
    """Load and bind-check the promotion manifest at `config.promotion_manifest_path`.

    Fail-closed, not fail-fast: any problem here -- the path missing,
    unreadable, malformed, schema-invalid, wrongly typed, phase-incomplete,
    identity-unbound, identity- or fingerprint-mismatched, or stale relative
    to `config.deployed_at_utc` -- degrades to display-only rather than
    raising. It never prevents server boot: every unexpected exception raised
    below is caught here and rendered as `manifest_malformed`, so fail-closed
    is a structural property of this boundary rather than of each individual
    check.

    Precedence of the identity binding changed in this release. Previously a
    `source_commit`, `source_tree_hash`, or `release_version` check was
    *skipped* when the corresponding `Config` field was falsy, so an
    unconfigured runtime accepted whatever identity the manifest declared.
    Now the comparisons are unconditional: an unset `Config` field means the
    runtime cannot prove the manifest describes this build, which resolves to
    `identity_unbound` (display-only) rather than silently passing.
    """
    try:
        return _load_promotion_manifest(config)
    except Exception:  # noqa: BLE001  # fail closed: a malformed manifest must never abort server boot
        return _unavailable("manifest_malformed")


def _load_promotion_manifest(config: Config) -> PromotionManifest:
    path = Path(config.promotion_manifest_path)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    # Same guard `phase4c_artifact_path` takes, and for the same reason: this
    # is operator config, and a path that names a device or FIFO would hang or
    # OOM-kill server boot on read_text(). This is the first evidence read the
    # boot path takes, so it is the one that must fail closed.
    if not _regular_file_within_evidence_size_cap(path):
        return _unavailable("manifest_missing")
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return _unavailable("manifest_missing")
    try:
        manifest = json.loads(raw)
    except json.JSONDecodeError:
        return _unavailable("manifest_malformed")
    if not isinstance(manifest, dict):
        return _unavailable("manifest_malformed")

    missing = _MANIFEST_REQUIRED_FIELDS - set(manifest)
    if missing:
        return _unavailable("manifest_schema_invalid")

    if any(not isinstance(manifest[name], str) for name in _MANIFEST_STRING_FIELDS):
        return _unavailable("manifest_schema_invalid")
    if not isinstance(manifest["promotion_eligible"], bool):
        return _unavailable("manifest_schema_invalid")
    if manifest["reason"] is not None and not isinstance(manifest["reason"], str):
        return _unavailable("manifest_schema_invalid")
    if not isinstance(manifest["inputs"], Mapping):
        return _unavailable("manifest_schema_invalid")
    if not _is_hex_hash(manifest["schema_hash"]):
        return _unavailable("manifest_schema_invalid")
    # `inputs` was previously accepted as "any mapping". Each entry names an
    # artifact this manifest claims to be bound to, so each must actually
    # carry a path and a well-formed digest -- otherwise a forged manifest
    # with a correct outer identity could declare arbitrary/empty bindings.
    for entry in manifest["inputs"].values():
        if not isinstance(entry, Mapping):
            return _unavailable("manifest_schema_invalid")
        path_value = entry.get("path")
        if not isinstance(path_value, str) or not path_value.strip():
            return _unavailable("manifest_schema_invalid")
        if not _is_hex_hash(entry.get("sha256")):
            return _unavailable("manifest_schema_invalid")
    schema_verdict = _schema_hash_matches(manifest["schema_hash"])
    if schema_verdict == "unverifiable":
        return _unavailable("manifest_schema_unverifiable")
    if schema_verdict == "mismatch":
        return _unavailable("manifest_schema_hash_mismatch")
    # `generated_at_utc` is parsed unconditionally: when no `deployed_at_utc`
    # is configured there is no staleness comparison to force the parse, and a
    # field whose format was never checked is not a verified field.
    try:
        generated_at = _parse_utc_timestamp(manifest["generated_at_utc"])
    except ValueError:
        return _unavailable("manifest_schema_invalid")

    manifest_phase = manifest["manifest_phase"]
    if manifest_phase not in {"provisional", "final"}:
        return _unavailable("manifest_schema_invalid")

    identity = PromotionManifest(
        promotion_eligible=False,
        manifest_phase=manifest_phase,
        source_commit=manifest["source_commit"],
        source_tree_hash=manifest["source_tree_hash"],
        release_version=manifest["release_version"],
        feature_policy_fingerprint=manifest["feature_policy_fingerprint"],
        generated_at_utc=manifest["generated_at_utc"],
    )

    if manifest_phase == "provisional":
        # A provisional manifest is accepted for diagnostics only; it can
        # never enable autoplay regardless of the evidence it embeds.
        return replace(identity, reason="provisional_manifest")

    # manifest_phase == "final": Phase 3 must have stamped its completion
    # hash, and any declared Phase 4C hash must validate; Phase 2 does not
    # implement that stamping, so a "final" manifest this loader can see is
    # necessarily incomplete until Phase 3 lands.
    if not _is_hex_hash(manifest.get("phase3_completion_hash")):
        return replace(identity, reason="incomplete_final_manifest")
    # The Phase 3 completion hash and the Phase 3 input binding are two
    # records of the same fact; a manifest whose top-level hash disagrees with
    # (or has no) corresponding `inputs.phase3` entry is internally
    # inconsistent and must not activate autoplay.
    phase3_entry = manifest["inputs"].get("phase3")
    if not isinstance(phase3_entry, Mapping):
        return replace(identity, reason="incomplete_final_manifest")
    if phase3_entry.get("sha256") != manifest["phase3_completion_hash"]:
        return replace(identity, reason="phase3_binding_mismatch")
    # A *final* manifest attests the whole evidence chain, so every earlier
    # phase must be bound too; only a provisional manifest may lack them.
    if not _MANIFEST_REQUIRED_FINAL_INPUTS <= set(manifest["inputs"]):
        return replace(identity, reason="incomplete_final_manifest")
    # Unlike Phase 4C (byte-verified below), the Phase 0/1/2/3 `inputs`
    # entries were previously only shape-checked (a non-empty path string, a
    # well-formed hex digest) -- nothing recomputed the digest against the
    # actual file on disk, so a `final` manifest's declared evidence hashes
    # were never actually proven to describe the files they name. Making the
    # Phase 4C byte-check the rule rather than the exception closes that gap.
    for _phase_name in _MANIFEST_REQUIRED_FINAL_INPUTS:
        _phase_entry = manifest["inputs"][_phase_name]
        _phase_path = _resolve_confined_evidence_path(_phase_entry["path"])
        if _phase_path is None:
            return replace(identity, reason="evidence_unresolvable")
        try:
            _actual_phase_hash = hashlib.sha256(_phase_path.read_bytes()).hexdigest()
        except OSError:
            return replace(identity, reason="evidence_unresolvable")
        if _actual_phase_hash != _phase_entry["sha256"]:
            return replace(identity, reason="evidence_mismatch")

    policy = FeaturePolicy.from_config(config)
    expected_fingerprint = feature_policy_fingerprint(policy)
    if manifest["feature_policy_fingerprint"] != expected_fingerprint:
        return replace(identity, reason="policy_fingerprint_mismatch")
    # Unconditional identity binding: an unset Config field cannot prove the
    # manifest describes this build, so it is unbound rather than waived.
    for config_value, manifest_key in (
        (config.source_commit, "source_commit"),
        (config.source_tree_hash, "source_tree_hash"),
        (config.release_version, "release_version"),
    ):
        if not config_value:
            return replace(identity, reason="identity_unbound")
        if manifest[manifest_key] != config_value:
            return replace(identity, reason="source_mismatch")
    deployed_at = config.deployed_at_utc
    if deployed_at and generated_at < _parse_utc_timestamp(deployed_at):
        return replace(identity, reason="stale")

    if manifest["promotion_eligible"] is not True:
        return replace(identity, reason=manifest["reason"] or "not_promotion_eligible")

    # An optional Phase 4C binding: absent, a no-change release is still a
    # valid final manifest. Present, the declared hash must resolve to a
    # readable file whose current bytes actually match it -- an unresolvable
    # or mismatched artifact is treated as stale/foreign, never trusted blindly.
    phase4c_hash = manifest.get("phase4c_artifact_sha256")
    if phase4c_hash is None:
        # Omitting the field entirely must not be indistinguishable from "no
        # Phase 4C attestation applies" when this deployment explicitly wants
        # one: if `config.phase4c_artifact_path` is configured, an otherwise
        # -valid `final` manifest that simply drops the field would silently
        # skip the byte-verification below rather than being caught as an
        # incomplete binding. The "both absent" case (no configured path,
        # no declared hash) is the genuine no-Phase-4C-needed release and
        # stays eligible.
        if config.phase4c_artifact_path:
            return replace(identity, reason="phase4c_binding_missing")
    else:
        # Present-but-unusable is a forged binding, not an absent one: `""` or a
        # malformed digest previously fell through the truthiness test and
        # skipped every Phase 4C check.
        if not _is_hex_hash(phase4c_hash):
            return replace(identity, reason="phase4c_binding_mismatch")
        phase4c_artifact_path = config.phase4c_artifact_path
        if not phase4c_artifact_path:
            return replace(identity, reason="phase4c_unresolvable")
        phase4c_path = Path(phase4c_artifact_path)
        if not phase4c_path.is_absolute():
            phase4c_path = _REPO_ROOT / phase4c_path
        try:
            # phase4c_artifact_path is operator config (env/TOML), not
            # manifest-declared, so it is not attacker-steerable the way the
            # phase0-3 `inputs[*].path` entries are -- but it is still an
            # operator-controllable path read on the server-boot path, and
            # `_resolve_confined_evidence_path`'s sibling reads apply the
            # same size/regular-file bound for exactly that reason: an
            # accidental device-file or FIFO path would otherwise make
            # `read_bytes()` block indefinitely (`/dev/zero` never reaches
            # EOF), hanging or OOM-killing server boot instead of degrading
            # to `phase4c_unresolvable` like every other unreadable-path
            # case here.
            if not _regular_file_within_evidence_size_cap(phase4c_path):
                return replace(identity, reason="phase4c_unresolvable")
            actual_hash = hashlib.sha256(phase4c_path.read_bytes()).hexdigest()
        except OSError:
            return replace(identity, reason="phase4c_unresolvable")
        if actual_hash != phase4c_hash:
            return replace(identity, reason="phase4c_mismatch")
        # The declared top-level hash and the `inputs.phase4c` binding are two
        # records of the same artifact; disagreement means the manifest was
        # assembled from mismatched parts.
        phase4c_entry = manifest["inputs"].get("phase4c")
        if not isinstance(phase4c_entry, Mapping) or phase4c_entry.get("sha256") != phase4c_hash:
            return replace(identity, reason="phase4c_binding_mismatch")

    return replace(identity, promotion_eligible=True, reason=None)


def _parse_strict_bool(raw: object, *, field: str) -> bool:
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False
    raise ConfigError(f"{field} must be 'true' or 'false'")


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
    for env_name, field_name in (
        ("WEBSEARCH_ENABLE_EARLY_ACK", "enable_early_ack"),
        ("WEBSEARCH_ENABLE_BACKGROUND_STATUS", "enable_background_status"),
        ("WEBSEARCH_ENABLE_AUTOPLAY_POLICY", "enable_autoplay_policy"),
    ):
        if env_name in values:
            kwargs[field_name] = _parse_strict_bool(values[env_name], field=env_name)
    # Membership, not truthiness: `__post_init__` rejects an empty
    # `early_ack_text`/`promotion_manifest_path`/`release_version`, and the
    # identity fields treat empty as unbound. A walrus-truthiness check
    # silently swallowed an explicitly-empty operator setting and substituted
    # the default, so the validation could never fire. This matches the
    # membership pattern the boolean flags above already use.
    for env_name, field_name in (
        ("WEBSEARCH_EARLY_ACK_TEXT", "early_ack_text"),
        ("WEBSEARCH_PROMOTION_MANIFEST_PATH", "promotion_manifest_path"),
        ("WEBSEARCH_PHASE4C_ARTIFACT_PATH", "phase4c_artifact_path"),
        ("WEBSEARCH_RELEASE_VERSION", "release_version"),
        ("PIPECAT_SOURCE_COMMIT", "source_commit"),
        ("PIPECAT_SOURCE_TREE_HASH", "source_tree_hash"),
        ("PIPECAT_DEPLOYED_AT_UTC", "deployed_at_utc"),
    ):
        if env_name in values:
            kwargs[field_name] = str(values[env_name])
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
    features = document.get("features", {})
    if not all(isinstance(section, Mapping) for section in (stt, tts, turn, models, features)):
        raise ConfigError(
            "[stt], [tts], [turn], [models], and [features] config sections must be tables"
        )
    for key in ("enable_early_ack", "enable_background_status", "enable_autoplay_policy"):
        if key in features:
            values[f"WEBSEARCH_{key.upper()}"] = features[key]
    if "early_ack_text" in features:
        values["WEBSEARCH_EARLY_ACK_TEXT"] = features["early_ack_text"]
    if "promotion_manifest_path" in features:
        values["WEBSEARCH_PROMOTION_MANIFEST_PATH"] = features["promotion_manifest_path"]
    if "phase4c_artifact_path" in features:
        values["WEBSEARCH_PHASE4C_ARTIFACT_PATH"] = features["phase4c_artifact_path"]
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

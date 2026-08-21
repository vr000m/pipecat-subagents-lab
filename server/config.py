"""Validated operator configuration; secrets are never part of its repr."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, replace
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _package_version
from ipaddress import ip_address
from math import isfinite
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

_FALLBACK_RELEASE_VERSION = "0.1.3"


def _reasoning_effort_literal_values() -> frozenset[str]:
    """Derive the valid ``reasoning.effort`` values from the installed OpenAI
    SDK's own ``ReasoningEffort`` alias, rather than hand-copying its Literal
    args -- a hand-copy silently desyncs on the next SDK bump (round 7
    gauntlet finding 13). ``ReasoningEffort`` is
    ``Optional[Literal["none", "minimal", ...]]``, i.e.
    ``Literal[...] | None`` -- not itself usable as a plain membership set --
    so this unwraps one level of ``get_args`` to reach the ``Literal``, then a
    second to reach its string args. Falls back to a pinned literal copy if
    the SDK ever restructures this alias in a way that breaks the two-level
    unwrap (e.g. drops the ``| None``), so a future SDK refactor degrades to
    stale-but-correct-at-time-of-writing rather than crashing config load.
    """
    try:
        from typing import get_args

        from openai.types.shared.reasoning_effort import ReasoningEffort

        literal_args = get_args(ReasoningEffort)
        values = frozenset(get_args(literal_args[0]))
        if not values or not all(isinstance(v, str) for v in values):
            raise TypeError("unexpected ReasoningEffort shape")
        return values
    except Exception:
        return frozenset({"none", "minimal", "low", "medium", "high", "xhigh", "max"})


_VALID_REASONING_EFFORTS = _reasoning_effort_literal_values()


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


def _reasoning_efforts(value: Mapping[str, str]) -> dict[str, str]:
    result = dict(value)
    for label, effort in result.items():
        if not label:
            raise ConfigError("reasoning-effort policies must contain non-empty labels")
        if effort not in _VALID_REASONING_EFFORTS:
            raise ConfigError(f"unsupported reasoning effort: {effort!r}")
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
    router_reasoning_effort_policy: Mapping[str, str] = field(default_factory=dict)
    worker_reasoning_effort_policy: Mapping[str, str] = field(default_factory=dict)

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
        if self.phase4c_artifact_path is not None and not self.phase4c_artifact_path.strip():
            raise ConfigError("phase4c_artifact_path must not be empty")
        if not self.release_version.strip():
            raise ConfigError("release_version must not be empty")
        if self.deployed_at_utc is not None:
            try:
                _parse_utc_timestamp(self.deployed_at_utc)
            except ValueError as exc:
                raise ConfigError("deployed_at_utc must be an ISO-8601 UTC timestamp") from exc
        object.__setattr__(self, "router_model_policy", _models(self.router_model_policy))
        object.__setattr__(self, "worker_model_policy", _models(self.worker_model_policy))
        object.__setattr__(
            self,
            "router_reasoning_effort_policy",
            _reasoning_efforts(self.router_reasoning_effort_policy),
        )
        object.__setattr__(
            self,
            "worker_reasoning_effort_policy",
            _reasoning_efforts(self.worker_reasoning_effort_policy),
        )
        # An effort-policy label that isn't a registered model-policy label
        # (a typo, e.g. "fastt" instead of "fast") would otherwise be silently
        # accepted and never read -- resolve_*_reasoning_effort only raises
        # for a *lookup* against an unregistered label, and nothing else in
        # this dataclass ever iterates the effort-policy dict's keys against
        # the model-policy dict's keys. Catching the mismatch here, at
        # construction, surfaces the typo immediately instead of leaving a
        # silently-inert config entry.
        _unknown_router_effort_labels = set(self.router_reasoning_effort_policy) - set(
            self.router_model_policy
        )
        if _unknown_router_effort_labels:
            raise ConfigError(
                "router_reasoning_effort_policy has labels not present in "
                f"router_model_policy: {sorted(_unknown_router_effort_labels)}"
            )
        _unknown_worker_effort_labels = set(self.worker_reasoning_effort_policy) - set(
            self.worker_model_policy
        )
        if _unknown_worker_effort_labels:
            raise ConfigError(
                "worker_reasoning_effort_policy has labels not present in "
                f"worker_model_policy: {sorted(_unknown_worker_effort_labels)}"
            )

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

    def resolve_router_reasoning_effort(self, policy_label: str) -> str | None:
        if policy_label not in self.router_model_policy:
            raise ConfigError(f"unknown router model policy: {policy_label}")
        return self.router_reasoning_effort_policy.get(policy_label)

    def resolve_worker_reasoning_effort(self, policy_label: str) -> str | None:
        if policy_label not in self.worker_model_policy:
            raise ConfigError(f"unknown worker model policy: {policy_label}")
        return self.worker_reasoning_effort_policy.get(policy_label)

    def with_discovered_endpoint(self, service: str, transport: str, address: str) -> Config:
        if service not in {"stt", "tts"}:
            raise ConfigError(f"unknown service: {service}")
        return replace(self, **{f"{service}_endpoint": (transport, address)})


def default_reasoning_effort_for_model(model: str) -> str | None:
    """The reasoning effort to send for ``model`` when no effort was configured.

    Model-naming rule only, deliberately independent of any one caller's
    policy: OpenAI's gpt-5* reasoning models otherwise default to a
    provider-side effort that can consume a small completion budget entirely
    on hidden reasoning tokens. Non-gpt-5* models get no effort param.

    Kept separate from ``server.router.effective_router_reasoning_effort``
    (which layers the ROUTER's resolved-config precedence on top of this) so
    retuning the router's own default policy cannot silently change an
    unrelated caller -- ``scripts/eval_common.py``'s judge builder shares this
    naming rule, not the router's policy (round 10 gauntlet, Architecture
    finding 4).
    """
    return "minimal" if model.startswith("gpt-5") else None


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


_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
"""Anchor for relative evidence/manifest paths: the directory this module's
package lives in, not a git-repo lookup (`git rev-parse --show-toplevel`) and
not the process CWD. In a repo checkout this happens to equal the repo root,
which is where the name `_REPO_ROOT` came from; it was renamed because a
packaged/standalone install of this server has no git tree at all, and the
old name implied an assumption this module never actually made. The
promotion-evidence tree (`docs/benchmarks/...`, `shared/schemas/...`) is
deliberately excluded from the deployable package (see
`_schema_hash_matches`), so a packaged install typically won't find these
paths under `_PACKAGE_ROOT` -- that's expected, and every caller below
degrades to a display-only fail-closed verdict (`manifest_missing`,
`manifest_schema_unverifiable`, `phase4c_unresolvable`, ...) rather than
raising. An operator deploying standalone who still wants promotion evidence
honoured must point `promotion_manifest_path`/`phase4c_artifact_path` at an
absolute path; a relative path is only meaningful inside a repo checkout.
"""

_EVIDENCE_SCHEMA_PATH = _PACKAGE_ROOT / "shared/schemas/v013-evidence.json"

# A manifest-declared evidence input is attacker-steerable (it lives inside the
# artifact under scrutiny), so it is capped well above any real evidence file
# to bound the read even once path confinement has already ruled out traversal.
_MAX_EVIDENCE_INPUT_BYTES = 8 * 1024 * 1024


def _read_regular_file_no_follow(path: Path, *, max_bytes: int) -> bytes | None:
    """Open, fstat-verify, and read `path`, returning `None` for anything that
    is not a size-bounded regular file -- or is unreadable at all.

    Runtime evidence paths here are either attacker-steerable (a manifest's
    own declared `inputs[*].path`) or operator-controllable (env/TOML
    settings) but still read on the server-boot path, so both threat models
    get the same treatment: a prior `Path.stat()`/`Path.is_file()` check
    followed by a *separate* `read_text()`/`read_bytes()` call leaves a
    TOCTOU window between the check and the read -- the path can be swapped
    (e.g. a regular file replaced by a symlink or FIFO) in between. Opening
    once with `O_NONBLOCK` and then `fstat`-ing the *held fd* closes that
    window: the type/size decision and the read happen against the exact
    same file, not against whatever now sits at the path's name.

    `O_NOFOLLOW` additionally rejects a symlink planted at the path outright
    (`ELOOP`) rather than silently resolving it, and `O_NONBLOCK` means
    opening (and, since the type check runs before any read, never reading)
    a FIFO or character device does not block or hang server boot the way a
    plain `open()`/`read_bytes()` would (`/dev/zero` never reaches EOF). The
    read loop is additionally capped at `max_bytes` so an oversized regular
    file is not slurped into memory either.

    Every failure mode here -- missing path, permission denied, symlinked,
    not a regular file, oversized -- collapses to `None`; callers already
    treat every evidence-read failure as one fail-closed outcome (e.g.
    `evidence_unresolvable`/`manifest_missing`/`phase4c_unresolvable`), so
    there is nothing for a caller to do differently per failure kind.
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        return None
    try:
        from stat import S_ISREG

        st = os.fstat(fd)
        if not S_ISREG(st.st_mode):
            return None
        chunks: list[bytes] = []
        total = 0
        while True:
            try:
                chunk = os.read(fd, 1 << 20)
            except OSError:
                return None
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                return None
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


def _resolve_confined_evidence_path(raw_path: str) -> Path | None:
    """Resolve a manifest-declared evidence path, confined to the repo tree.

    `raw_path` comes from `manifest["inputs"][phase]["path"]`, a value the
    artifact under validation declares about itself -- not an operator
    setting. Accepting it as a runtime read target without containment would
    be an attacker-steerable arbitrary-file-read primitive (absolute paths,
    `..` traversal). Returns `None` when the path is absolute or escapes the
    repo root after resolution.

    Deliberately does *not* also check "is a regular file within the size
    cap" here: doing that with a `stat()` call and then reading the path
    separately at the caller is exactly the TOCTOU gap
    `_read_regular_file_no_follow` exists to close, so every caller must
    route the actual read through that helper, which re-derives both facts
    from the fd it opens for the read.
    """
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return None
    resolved = (_PACKAGE_ROOT / candidate).resolve()
    if not resolved.is_relative_to(_PACKAGE_ROOT):
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
        path = _PACKAGE_ROOT / path
    # Same guard `phase4c_artifact_path` takes, and for the same reason: this
    # is operator config, and a path that names a device or FIFO would hang or
    # OOM-kill server boot on read_text(). This is the first evidence read the
    # boot path takes, so it is the one that must fail closed. Routed through
    # `_read_regular_file_no_follow` (a single open+fstat+read, not a
    # `stat()`-then-`read_text()` pair) so the type/size decision and the
    # bytes actually read cannot be split across a TOCTOU window.
    content = _read_regular_file_no_follow(path, max_bytes=_MAX_EVIDENCE_INPUT_BYTES)
    if content is None:
        return _unavailable("manifest_missing")
    try:
        raw = content.decode("utf-8")
    except UnicodeDecodeError:
        return _unavailable("manifest_malformed")
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
        # `_read_regular_file_no_follow`, not `_phase_path.read_bytes()`: a
        # single open+fstat+read closes the TOCTOU window a separate
        # regular-file/size check followed by a read would leave open.
        _phase_content = _read_regular_file_no_follow(
            _phase_path, max_bytes=_MAX_EVIDENCE_INPUT_BYTES
        )
        if _phase_content is None:
            return replace(identity, reason="evidence_unresolvable")
        _actual_phase_hash = hashlib.sha256(_phase_content).hexdigest()
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
            phase4c_path = _PACKAGE_ROOT / phase4c_path
        # phase4c_artifact_path is operator config (env/TOML), not
        # manifest-declared, so it is not attacker-steerable the way the
        # phase0-3 `inputs[*].path` entries are -- but it is still an
        # operator-controllable path read on the server-boot path, and
        # `_read_regular_file_no_follow` applies the same size/regular-file
        # bound, from one held fd, for exactly that reason: an accidental
        # device-file or FIFO path would otherwise make `read_bytes()` block
        # indefinitely (`/dev/zero` never reaches EOF), and a separate
        # `stat()`-then-`read_bytes()` pair would leave a TOCTOU window
        # between the check and the read -- hanging or OOM-killing server
        # boot, or reading through a swapped-in symlink, instead of
        # degrading to `phase4c_unresolvable` like every other
        # unreadable-path case here.
        phase4c_content = _read_regular_file_no_follow(
            phase4c_path, max_bytes=_MAX_EVIDENCE_INPUT_BYTES
        )
        if phase4c_content is None:
            return replace(identity, reason="phase4c_unresolvable")
        actual_hash = hashlib.sha256(phase4c_content).hexdigest()
        if actual_hash != phase4c_hash:
            return replace(identity, reason="phase4c_mismatch")
        # The declared top-level hash and the `inputs.phase4c` binding are two
        # records of the same artifact; disagreement means the manifest was
        # assembled from mismatched parts.
        phase4c_entry = manifest["inputs"].get("phase4c")
        if not isinstance(phase4c_entry, Mapping) or phase4c_entry.get("sha256") != phase4c_hash:
            return replace(identity, reason="phase4c_binding_mismatch")

    return replace(identity, promotion_eligible=True, reason=None)


def _registered_policy_labels(kwargs: dict[str, object], field_name: str) -> tuple[str, ...]:
    """The label(s) about to be registered for ``field_name`` (a
    ``*_model_policy`` field) in the ``Config`` this ``kwargs`` dict will
    construct -- ``kwargs[field_name]``'s own keys if this same
    ``load_config()`` call already populated it (e.g. via
    ``WEBSEARCH_ROUTER_MODEL``/``WEBSEARCH_WORKER_MODEL``), else the
    dataclass field's own default label(s). Lets a reasoning-effort env
    override apply to whatever label is actually in play instead of a
    hardcoded guess (round 8 gauntlet, Logic lens finding 4).

    Forward-compatibility, not a currently-reachable path: today
    ``load_config()``'s ``WEBSEARCH_ROUTER_MODEL``/``WEBSEARCH_WORKER_MODEL``
    overrides always hardcode ``{"fast": ...}``/``{"deep": ...}`` (see the
    call sites below), so this function's "``kwargs[field_name]``'s own
    keys" branch never actually observes a custom label from a real
    ``load_config()`` call -- it only guards against a future TOML/env
    surface that lets an operator supply one (round 9 gauntlet, Logic lens
    finding 9).
    """
    registered = kwargs.get(field_name)
    if isinstance(registered, Mapping):
        return tuple(registered)
    default_factory = next(f for f in fields(Config) if f.name == field_name).default_factory
    assert callable(default_factory)
    return tuple(default_factory())


_ROLE_MODEL_EFFORT_KEYS: tuple[tuple[str, str], ...] = (
    ("WEBSEARCH_ROUTER_MODEL", "WEBSEARCH_ROUTER_REASONING_EFFORT"),
    ("WEBSEARCH_WORKER_MODEL", "WEBSEARCH_WORKER_REASONING_EFFORT"),
)
# Both derived from _ROLE_MODEL_EFFORT_KEYS, and their membership is
# identical today by construction -- two names because they encode two
# different rules with two different justifications, not because the sets
# are expected to diverge on this checkout:
#   _PROVENANCE_KEYS: which keys carry per-layer provenance, so
#   _clear_inherited_reasoning_effort can tell "a higher layer set the model"
#   from "the model came along with the effort".
#   _EMPTY_MEANS_ABSENT_KEYS: which keys treat an effectively-empty override
#   as "absent" rather than as a write that erases the lower layer -- because
#   THIS KEY'S CONSUMER reads it with a truthiness walrus, so empty already
#   means absent there too. A key whose consumer does a plain `in values`
#   presence check must NOT be listed here.
# A future key that satisfies one rule but not the other is added to that
# constant alone, and separate *objects* alone buy nothing towards that --
# round 4 restart's Architecture Minor #7 made them separate comprehensions
# instead of an alias, but both still derive from the same source tuple, so
# adding a role still changes both simultaneously; that was never load-
# bearing independence. What actually matters, and what
# TestProvenanceKeysAloneDriveLayerRecording /
# TestEmptyMeansAbsentKeysAloneDriveTheEmptyOverrideSkip in tests/test_config.py
# pin, is that each constant is read by exactly one function: _PROVENANCE_KEYS
# by _record_layer, _EMPTY_MEANS_ABSENT_KEYS by _apply_layer (round 5
# restart2, Architecture A6).
_PROVENANCE_KEYS = tuple(k for pair in _ROLE_MODEL_EFFORT_KEYS for k in pair)
_EMPTY_MEANS_ABSENT_KEYS = tuple(k for pair in _ROLE_MODEL_EFFORT_KEYS for k in pair)


def _effectively_set(value: object) -> bool:
    """Whether an override should count as a real value.

    Deliberately STRICTER than the truthiness-walrus consumers at
    ``load_config``'s model/effort override block: those read
    ``if raw := values.get("WEBSEARCH_ROUTER_MODEL")``, for which a
    whitespace-only ``"   "`` is truthy, whereas the ``.strip()`` here treats
    it as absent. The divergence is intentional and one-directional -- a
    whitespace-only model ID or reasoning effort is nonsense at every
    consumer, so absorbing it here is better than propagating it. Everything
    the consumers treat as absent (``None``, ``""``) is absent here too, so
    the strictness can never resurrect a lower layer the consumers would have
    used (round 3 confirming pass, Logic finding 3).

    Also matches the consumers in the falsy-non-string direction: env vars
    are always strings, but a typed TOML value can be ``False``/``0``/``[]``
    -- those are truthy under the OLD ``value is not None`` check (so
    ``str(value)`` -> ``"False"``/``"0"``, both non-empty) even though every
    consumer's truthiness walrus treats them as absent. That divergence let a
    typed-TOML ``router_model = false`` record real provenance in
    ``_record_layer`` (and pass ``_apply_layer``'s write gate) for a value no
    consumer would ever read, which ``_clear_inherited_reasoning_effort``
    could then act on to delete a real lower-layer effort key. ``bool(value)``
    restores the stated one-directional invariant exactly: falsy in the same
    sense the consumers are, not just falsy-when-stringified (round 5
    restart2, Logic L6).
    """
    return bool(value) and str(value).strip() != ""


def _record_layer(values: Mapping[str, object], layers: dict[str, int], layer: int) -> None:
    for key in _PROVENANCE_KEYS:
        if key in values and _effectively_set(values[key]):
            layers[key] = layer


def _apply_layer(
    values: dict[str, object],
    layer_values: Mapping[str, object],
    layers: dict[str, int],
    layer: int,
) -> None:
    """Merge one precedence layer into ``values`` and record its provenance.

    An effectively-empty override of a role model/effort key (_EMPTY_MEANS_ABSENT_KEYS)
    is a no-op, not a write. Every consumer of those four keys reads them with a
    truthiness walrus (load_config's model/effort block), so an empty value
    already means "absent" there -- but letting it land in ``values`` still
    ERASED the lower layer's real value, while _record_layer correctly ignored it
    for provenance. Net effect: `WEBSEARCH_WORKER_MODEL=""` over a TOML
    worker_model/worker_reasoning_effort pair kept the TOML effort (correct) but
    repaired it to the dataclass-DEFAULT model, discarding the TOML model the
    effort was configured against (round 11 gauntlet, Minor B).
    """
    for key, value in layer_values.items():
        if key in _EMPTY_MEANS_ABSENT_KEYS and not _effectively_set(value):
            continue
        values[key] = value
    _record_layer(layer_values, layers, layer)


def _clear_inherited_reasoning_effort(values: dict[str, object], layers: dict[str, int]) -> None:
    """Drop a lower-precedence-layer reasoning effort when a HIGHER-precedence
    layer overrode only that role's model.

    Without this, ``WEBSEARCH_WORKER_MODEL=gpt-5`` (the documented revert-to-
    baseline recipe) still inherited config.toml's ``worker_reasoning_effort =
    "medium"``, so the "baseline" request carried a ``reasoning`` param the
    real baseline never sends.

    Known second instance of the same concern, not yet migrated to layer
    provenance: the ``WEBSEARCH_TTS_WS_URI``/``_SOCKET``/``_HOST``/``_PORT``
    family below carries the same "a higher layer overriding one related key
    should clear an inherited sibling" shape, handled by an older idiom
    instead of this function's pattern (round-4 restart, Architecture Minor
    #8 -- signpost only, no behaviour change on this branch).
    """
    for model_key, effort_key in _ROLE_MODEL_EFFORT_KEYS:
        if effort_key not in values:
            continue
        if layers.get(model_key, -1) > layers.get(effort_key, -1):
            del values[effort_key]


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
    layers: dict[str, int] = {}
    _record_layer(values, layers, 0)
    if env_file:
        if isinstance(env_file, (str, Path)):
            env_file_values: dict[str, object] = {}
            for line in Path(env_file).read_text().splitlines():
                if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_file_values[key.strip()] = value.strip().strip("\"'")
        else:
            env_file_values = dict(env_file)
        _apply_layer(values, env_file_values, layers, 1)
    env_values = os.environ if env is None else env
    _apply_layer(values, env_values, layers, 2)
    # Overriding only a role's *model* at a higher-precedence layer than the
    # one that set its *effort* must not silently carry that inherited
    # effort along -- see _clear_inherited_reasoning_effort's docstring.
    _clear_inherited_reasoning_effort(values, layers)
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
            raw_value = values[env_name]
            if not isinstance(raw_value, str):
                raise ConfigError(f"{env_name} must be a string")
            kwargs[field_name] = raw_value
    if raw := values.get("WEBSEARCH_OPENAI_API_KEY_ENV"):
        kwargs["openai_api_key_env"] = raw
    if raw := values.get("WEBSEARCH_ROUTER_MODEL"):
        kwargs["router_model_policy"] = {"fast": str(raw)}
    if raw := values.get("WEBSEARCH_WORKER_MODEL"):
        kwargs["worker_model_policy"] = {"deep": str(raw)}
    # Truthiness walrus, matching the sibling WEBSEARCH_ROUTER_MODEL/
    # WEBSEARCH_WORKER_MODEL pattern just above -- not a presence check
    # (`"X" in values`), which treated an explicitly-empty override as a
    # request to hard-fail config validation instead of "absent" (round 7
    # gauntlet finding 7).
    #
    # If a higher-precedence layer overrode only this role's model,
    # _clear_inherited_reasoning_effort() already deleted this role's effort
    # key from `values` above, so `values.get(...)` here correctly sees it as
    # absent instead of inheriting a lower-layer default (round 10 gauntlet
    # finding 2).
    #
    # Applied to whatever label(s) are actually registered in the
    # corresponding model policy (kwargs["*_model_policy"] if this same
    # load_config() call already set one via WEBSEARCH_ROUTER_MODEL/
    # WEBSEARCH_WORKER_MODEL above, else the dataclass default's own
    # label), not hardcoded to "fast"/"deep" -- replacing the whole
    # effort-policy dict with a single hardcoded label previously tripped
    # __post_init__'s label cross-validation (:243-258) for any supported
    # non-default-label model-policy configuration, since that label would
    # never appear in worker_model_policy's/router_model_policy's own keys
    # (round 8 gauntlet, Logic lens finding 4).
    if raw := values.get("WEBSEARCH_ROUTER_REASONING_EFFORT"):
        kwargs["router_reasoning_effort_policy"] = {
            label: str(raw) for label in _registered_policy_labels(kwargs, "router_model_policy")
        }
    if raw := values.get("WEBSEARCH_WORKER_REASONING_EFFORT"):
        kwargs["worker_reasoning_effort_policy"] = {
            label: str(raw) for label in _registered_policy_labels(kwargs, "worker_model_policy")
        }
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
    if "release_version" in features:
        # Documented in README alongside WEBSEARCH_RELEASE_VERSION. Without
        # this copy an operator's [features].release_version was silently
        # dropped and load_config fell back to the packaged default, so the
        # promotion manifest's release_version bind check compared against the
        # wrong identity.
        values["WEBSEARCH_RELEASE_VERSION"] = features["release_version"]
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
    # Mapped through the same WEBSEARCH_ROUTER_REASONING_EFFORT/
    # WEBSEARCH_WORKER_REASONING_EFFORT keys the env-var path uses (see
    # load_config()'s handling below) -- this dict is consulted before
    # env-file/process-env values are merged in, so an explicit env
    # override still wins over a TOML default, same precedence as
    # router_model/worker_model above.
    if "router_reasoning_effort" in models:
        values["WEBSEARCH_ROUTER_REASONING_EFFORT"] = models["router_reasoning_effort"]
    if "worker_reasoning_effort" in models:
        values["WEBSEARCH_WORKER_REASONING_EFFORT"] = models["worker_reasoning_effort"]
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

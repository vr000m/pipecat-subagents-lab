"""Validated operator configuration; secrets are never part of its repr."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, fields, replace
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _package_version
from ipaddress import ip_address
from math import isfinite
from pathlib import Path
from stat import S_ISREG
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
    combination is stale. The server-side consumer of this binding
    (`load_promotion_manifest`) was retired along with the query-context
    promotion chain, but `scripts/validate_v013_evidence.py --verify-manifest`
    still re-derives this fingerprint to check the frozen committed v0.1.3
    manifest for drift.
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


def effective_feature_policy_fingerprint(config: Config) -> str:
    """`feature_policy_fingerprint(FeaturePolicy.from_config(config))`, named.

    Two independent callers -- ``scripts/emit_v013_deployment_metadata.py``
    (the writer, which stamps this into the release-metadata shell exports)
    and ``scripts/validate_v013_evidence.py`` (the verifier, which
    re-derives it to check manifest drift against the frozen committed
    v0.1.3 manifest) -- must resolve this composition identically by
    construction, not by each happening to spell the same three-call chain
    the same way. Previously the writer wrapped it in a script-local
    ``feature_policy_fingerprint_value()`` (with a function-local
    ``server.config`` import) while the verifier spelled the same composition
    inline; this is the one home for it, since both the writer and the
    verifier already import from ``server.config`` (round-5 restart,
    Architecture finding).
    """
    return feature_policy_fingerprint(FeaturePolicy.from_config(config))


MANIFEST_REQUIRED_FIELDS = frozenset(
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
MANIFEST_REQUIRED_FINAL_INPUTS = frozenset({"phase0", "phase1", "phase2", "phase3"})

# ...and what a *provisional* manifest must cover. Spelled as its own literal,
# NOT as `MANIFEST_REQUIRED_FINAL_INPUTS - {"phase3"}`: a subtraction makes
# every phase added to the `final` roster silently widen the provisional one
# too, which is exactly the coupling naming this constant was meant to break.
# Moving that subtraction from the consumer into this module (round 6 confirm
# pass 3) relocated the drift rather than removing it. The parity assertion
# below is what keeps the two rosters honest: widening `final` still passes,
# but *narrowing* it without narrowing this one fails at import
# (round 7 confirm pass 4, Logic Minor).
MANIFEST_REQUIRED_PROVISIONAL_INPUTS = frozenset({"phase0", "phase1", "phase2"})

# A provisional manifest is a final manifest still missing its in-flight
# bindings, never a manifest required to declare a phase `final` does not.
#
# Raised, not `assert`ed: `python -O`/`PYTHONOPTIMIZE` strips an assert at
# compile time, and this is the only mechanism covering the "narrow `final`
# without narrowing provisional" direction now that the subtraction is gone --
# a guard that disappears under a supported interpreter flag is not a guard
# (round 8 confirm pass 5, Logic Minor).
if not MANIFEST_REQUIRED_PROVISIONAL_INPUTS <= MANIFEST_REQUIRED_FINAL_INPUTS:
    raise ValueError(
        "MANIFEST_REQUIRED_PROVISIONAL_INPUTS must stay a subset of "
        "MANIFEST_REQUIRED_FINAL_INPUTS; "
        f"extra: {sorted(MANIFEST_REQUIRED_PROVISIONAL_INPUTS - MANIFEST_REQUIRED_FINAL_INPUTS)}"
    )


# Manifest keys whose value must be a JSON string. `reason` is deliberately
# excluded: it is required to be present but is null on an eligible manifest.
MANIFEST_STRING_FIELDS = (
    "manifest_phase",
    "schema_hash",
    "source_commit",
    "source_tree_hash",
    "release_version",
    "feature_policy_fingerprint",
    "deployed_at_utc",
    "generated_at_utc",
)


def read_regular_file_no_follow(path: Path, *, max_bytes: int) -> bytes | None:
    """Open, fstat-verify, and read `path`, returning `None` for anything that
    is not a size-bounded regular file -- or is unreadable at all.

    **Public, despite living in a configuration module.** It was named
    `_read_regular_file_no_follow` while `config.py` was its only consumer;
    `server/session_state.py` now reads
    `shared/work-status-retention.json` through it too, so it is part of the
    package's internal contract rather than config-private and reaching into
    it under an underscore name misstated that (round-4 confirm pass,
    Architecture finding). It stays in `config.py` rather than moving to a
    dedicated module because `config.py` deliberately has no intra-package
    imports -- it is the leaf every other `server` module may import -- and
    splitting the hardened read out would make it the first.

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
    treat every read failure as one fail-closed outcome (e.g.
    `server/session_state.py`'s retention-config load falling back to
    `_RETENTION_FALLBACK`), so there is nothing for a caller to do
    differently per failure kind.

    **Deliberately not shared with `scripts/evidence_common.read_bytes_no_follow`,
    which performs the same open/fstat/bounded-read sequence.** Two reasons,
    both structural rather than incidental:

    1. `server/` must stay deployable without the `scripts/` package, and
       `scripts/` must stay free of `server.*` imports (the evidence gates run
       in checkouts with no pipecat runtime installed). Neither direction of
       import is available, so "share it" would mean introducing a third
       top-level package purely for two ~25-line functions.
    2. The two have *opposite* error contracts by design. This one collapses
       every failure to `None` because it runs on the server-boot path, where
       the only correct response to unreadable evidence is a fail-closed
       display-only verdict. `read_bytes_no_follow` raises `EvidenceGateError`
       because it runs in a CLI gate, where the only correct response is a
       loud non-zero exit. Unifying would force one contract onto both call
       sites and lose that distinction.

    What *must* stay in step is the hardening itself -- the flag set
    (`O_RDONLY|O_NOFOLLOW|O_NONBLOCK`), the `fstat`-on-the-held-fd
    `S_ISREG` check, the chunked read, and the byte cap. A change to any of
    those here needs the mirrored change in
    `scripts/evidence_common._iter_file_chunks`, and vice versa.
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        return None
    try:
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
# Two names because they encode two different rules with two different
# justifications. They are currently equal by construction, and the comment
# below anticipates that they may diverge:
#   _PROVENANCE_KEYS: which keys carry per-layer provenance, so
#   _clear_inherited_reasoning_effort can tell "a higher layer set the model"
#   from "the model came along with the effort".
#   _EMPTY_MEANS_ABSENT_KEYS: which keys treat an effectively-empty override
#   as "absent" rather than as a write that erases the lower layer -- because
#   THIS KEY'S CONSUMER reads it with a truthiness walrus, so empty already
#   means absent there too. A key whose consumer does a plain `in values`
#   presence check must NOT be listed here.
#   This precondition is convention-only and deliberately unenforced: every
#   executable form of it is either tautological (an empty-override behaviour
#   test just re-tests _apply_layer's own skip) or source introspection over
#   load_config's consumer block. Accepted because all members derive from
#   _ROLE_MODEL_EFFORT_KEYS, whose membership is pinned by
#   tests/test_config.py's constant-parity test -- adding a key here cannot
#   happen without a human edit that trips that test (round 6, Architecture A2).
# A key that satisfies one rule but not the other is added to that constant
# alone. What matters, and what
# TestProvenanceKeysAloneDriveLayerRecording /
# TestEmptyMeansAbsentKeysAloneDriveTheEmptyOverrideSkip in tests/test_config.py
# pin, is that each constant is read by exactly one function: _PROVENANCE_KEYS
# by _record_layer, _EMPTY_MEANS_ABSENT_KEYS by _apply_layer (round 5
# restart2, Architecture A6).
# The STT/TTS endpoint keys are deliberately NOT here: they are alternative
# spellings of one setting rather than a dependent pair, and they resolve
# against the raw per-layer mappings through _family_layers -- see
# _winning_family_member. Routing them through _record_layer instead needed a
# hand-written key roster mirroring the member tuples 400 lines away (nothing
# pinned the two together), and inherited _effectively_set's truthiness, under
# which an explicit `tts_ws_port = 0` recorded no layer at all (round 6
# confirm pass 3, Architecture/Logic Minor).
_PROVENANCE_KEYS = tuple(k for pair in _ROLE_MODEL_EFFORT_KEYS for k in pair)
_EMPTY_MEANS_ABSENT_KEYS = tuple(k for pair in _ROLE_MODEL_EFFORT_KEYS for k in pair)
# The model half of _ROLE_MODEL_EFFORT_KEYS alone -- used by _record_layer to
# decide *whether to compare* a key's incoming value against its incumbent
# before advancing provenance (round 10 gauntlet, Logic finding 3). Effort
# keys are deliberately NOT in this set; see _record_layer's docstring for
# why that asymmetry is load-bearing, not an oversight.
_ROLE_MODEL_KEYS = tuple(model_key for model_key, _effort_key in _ROLE_MODEL_EFFORT_KEYS)


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


def _record_layer(
    values: Mapping[str, object],
    layers: dict[str, int],
    layer: int,
    incumbent_models: Mapping[str, object],
) -> None:
    """Record which layer last set each provenance key -- asymmetric between
    model keys and effort keys (round 10 gauntlet, Logic finding 3).

    Model keys (``_ROLE_MODEL_KEYS``) only advance provenance when this
    layer's effectively-set value actually CHANGES the effective model, per
    ``incumbent_models`` -- the merged value each model key held immediately
    before this layer's ``_apply_layer`` merge ran. Without this, re-
    asserting a role's model to its already-shipped value (concrete trigger:
    the justfile ``run`` recipe sourcing an env file that pins
    ``WEBSEARCH_WORKER_MODEL=gpt-5.6-terra``, identical to config.toml's
    default) still advanced provenance for that model key, and
    ``_clear_inherited_reasoning_effort`` then deleted the shipped
    ``worker_reasoning_effort`` purely because the model's layer number was
    higher than the effort's -- even though the model didn't actually
    change. Comparing against the incumbent removes the silent drop instead
    of merely narrating it (option (b), logging the drop, was declined:
    ``server/config.py`` has no logger and no logging import today, and
    introducing one into config loading for a diagnostic is a larger change
    than this fix).

    Effort keys deliberately keep the OLD unconditional
    advance-on-effectively-set rule -- this asymmetry is NOT applied
    symmetrically to effort keys, and that is the non-obvious part of this
    fix. Counter-example: TOML sets ``model=A, effort=medium``; env
    re-asserts ``model=B, effort=medium`` (a genuine model change AND an
    explicit effort re-assertion at the same, highest layer). A symmetric
    "only advance on change" rule would leave the effort's provenance at
    layer 0 (its value didn't change either), and
    ``_clear_inherited_reasoning_effort`` would then delete an effort the
    HIGHEST layer just explicitly re-asserted -- a new bug, not a fix. So an
    effort key's provenance still advances whenever it is effectively set,
    regardless of whether its value changed.

    Residual, documented rather than silently left: when NO lower layer set
    the model at all, ``incumbent_models`` holds ``None`` for that key, this
    layer's value never equals ``None`` (``_effectively_set`` already
    filters ``None``/empty out), so provenance advances exactly as before,
    and the inherited effort is still cleared. That covers a config.toml
    that sets an effort but not the paired model; comparing against the
    ``Config`` dataclass default instead would require resolving
    ``*_model_policy`` defaults before ``kwargs`` is built, which is out of
    proportion here. On this branch ``config.toml`` sets both role models,
    so the reported trigger is fully covered.

    ``_PROVENANCE_KEYS`` remains the sole gate on *which* keys are recorded
    at all -- unchanged, so ``test_provenance_keys_alone_drive_layer_
    recording``'s zeroing test still holds. ``_ROLE_MODEL_KEYS`` only
    decides *whether to compare* for a key already selected by that gate.
    """
    for key in _PROVENANCE_KEYS:
        if key not in values or not _effectively_set(values[key]):
            continue
        if key in _ROLE_MODEL_KEYS and values[key] == incumbent_models.get(key):
            # Same-value re-assertion: this layer didn't change the
            # effective model, so it must not advance provenance past
            # whichever earlier layer actually set it.
            continue
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

    This is now the sole merge path for all three precedence layers, including
    layer 0 (TOML). Layer 0 gains nothing *behaviourally* from the empty-
    override gate today -- it is the bottom layer, so an "erasing" write has
    nothing beneath it to erase -- but routing it through this function keeps
    the invariant "an effectively-empty role model/effort override is never a
    write" uniform across every layer, rather than true for layers 1-2 and
    merely incidental for layer 0. That uniformity is what stops a future
    layer added below TOML from silently reintroducing this bug class (round 6
    gauntlet, Logic G2 / Architecture A1).
    Captures each role model key's incumbent value BEFORE this layer's merge
    loop runs, and threads it into ``_record_layer`` -- see that function's
    docstring for why model-key provenance must compare against the
    incumbent while effort-key provenance must not (round 10 gauntlet,
    Logic finding 3).
    """
    incumbent_models = {key: values.get(key) for key in _ROLE_MODEL_KEYS}
    for key, value in layer_values.items():
        if key in _EMPTY_MEANS_ABSENT_KEYS and not _effectively_set(value):
            continue
        values[key] = value
    _record_layer(layer_values, layers, layer, incumbent_models)


def _clear_inherited_reasoning_effort(values: dict[str, object], layers: dict[str, int]) -> None:
    """Drop a lower-precedence-layer reasoning effort when a HIGHER-precedence
    layer overrode only that role's model.

    Without this, ``WEBSEARCH_WORKER_MODEL=gpt-5`` (the documented revert-to-
    baseline recipe) still inherited config.toml's ``worker_reasoning_effort =
    "medium"``, so the "baseline" request carried a ``reasoning`` param the
    real baseline never sends.

    The STT/TTS endpoint families carry the same "a higher layer overriding
    one related key should beat an inherited sibling" shape; they resolve it
    through ``_winning_family_member`` rather than by deletion, because there
    the sibling keys are alternative spellings of one setting, not a dependent
    pair. The vendor-key alias families look like that shape but deliberately
    do not take the rule -- see ``_winning_alias_key``.
    """
    for model_key, effort_key in _ROLE_MODEL_EFFORT_KEYS:
        if effort_key not in values:
            continue
        if layers.get(model_key, -1) > layers.get(effort_key, -1):
            del values[effort_key]


#: A "family" is several mutually-exclusive spellings of ONE setting, resolved
#: by :func:`_winning_family_member`. Declared here, once, rather than as
#: literals inside ``load_config``: the resolver needs the same key names twice
#: (to test presence and to compute each member's layer), and a spelling added
#: to one copy but not the other used to fail silently.
_STT_ENDPOINT_MEMBERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("endpoint", ("WEBSEARCH_STT_ENDPOINT",)),
    ("socket", ("WEBSEARCH_STT_WS_SOCKET",)),
)
#: Declaration order is the README's documented within-layer key priority:
#: ``ENDPOINT`` > ``WS_URI`` > ``WS_SOCKET`` > ``WS_HOST``+``WS_PORT``.
_TTS_ENDPOINT_MEMBERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("endpoint", ("WEBSEARCH_TTS_ENDPOINT",)),
    ("uri", ("WEBSEARCH_TTS_WS_URI",)),
    ("socket", ("WEBSEARCH_TTS_WS_SOCKET",)),
    ("host_port", ("WEBSEARCH_TTS_WS_HOST", "WEBSEARCH_TTS_WS_PORT")),
)


def _build_stt_endpoint(member: str | None, values: Mapping[str, object]) -> tuple[str, str] | None:
    """The STT endpoint the winning family member names, or ``None`` for none."""
    if member == "endpoint":
        return parse_endpoint(str(values["WEBSEARCH_STT_ENDPOINT"]))
    if member == "socket":
        return ("uds", _expand_socket(str(values["WEBSEARCH_STT_WS_SOCKET"])))
    return None


def _build_tts_endpoint(member: str | None, values: Mapping[str, object]) -> tuple[str, str] | None:
    """The TTS endpoint the winning family member names, or ``None`` for none.

    Raises ``ConfigError`` for a ``host_port`` member whose port is not an
    integer in range -- the same loud failure every other malformed endpoint
    input here gets, rather than a silent fall-through to the packaged default.
    """
    if member == "endpoint":
        return parse_endpoint(str(values["WEBSEARCH_TTS_ENDPOINT"]))
    if member == "uri":
        return parse_endpoint(str(values["WEBSEARCH_TTS_WS_URI"]))
    if member == "socket":
        return ("uds", _expand_socket(str(values["WEBSEARCH_TTS_WS_SOCKET"])))
    if member == "host_port":
        try:
            port = int(str(values.get("WEBSEARCH_TTS_WS_PORT")))
        except ValueError as exc:
            raise ConfigError("WEBSEARCH_TTS_WS_PORT must be an integer") from exc
        if not 1 <= port <= 65_535:
            raise ConfigError("WEBSEARCH_TTS_WS_PORT must be between 1 and 65535")
        return ("ws", f"{values.get('WEBSEARCH_TTS_WS_HOST')}:{port}")
    return None


#: The endpoint families, ``(config field, members, builder)``. THE roster
#: ``load_config`` iterates to resolve them AND to consume the result -- it no
#: longer names the member constants directly, so a third endpoint family
#: declared here reaches resolution, consumption, and :func:`_families` in one
#: edit (round 8 confirm pass 5 / round 9 confirm pass 6, Architecture).
#:
#: The builder is what closes the consumption half: with the per-member
#: ``kwargs`` branches left hand-written, a newly registered family resolved
#: correctly and was then silently dropped, because nothing read its entry back
#: out and the operator's configured endpoint was replaced by the dataclass
#: default. A row without a builder is now a declaration-site omission
#: (a missing tuple element), not a silent runtime one.
_ENDPOINT_FAMILIES: tuple[
    tuple[
        str,
        tuple[tuple[str, tuple[str, ...]], ...],
        Callable[[str | None, Mapping[str, object]], tuple[str, str] | None],
    ],
    ...,
] = (
    ("stt_endpoint", _STT_ENDPOINT_MEMBERS, _build_stt_endpoint),
    ("tts_endpoint", _TTS_ENDPOINT_MEMBERS, _build_tts_endpoint),
)

#: The env var naming the OpenAI family's bare spelling when the operator has
#: not redirected it with ``WEBSEARCH_OPENAI_API_KEY_ENV``.
_DEFAULT_OPENAI_KEY_ENV = "OPENAI_API_KEY"

#: The vendor-credential alias families as ``(config field, scoped key, bare
#: key)`` rows. ``None`` for the bare key means "this family's bare spelling is
#: named at runtime" -- :func:`_alias_families` substitutes its argument there.
#: That property lives in the data rather than in a ``field_name ==
#: "openai_api_key"`` comparison inside the generic builder, so a second
#: runtime-redirectable credential needs a row, not a branch (round 8 confirm
#: pass 5, Architecture Minor).
#:
#: These resolve through :func:`_winning_alias_key`, NOT
#: :func:`_winning_family_member` -- see that function for why credential
#: aliases do not take the endpoint families' layer-first rule.
_ALIAS_FAMILY_SPELLINGS: tuple[tuple[str, str, str | None], ...] = (
    ("openai_api_key", "WEBSEARCH_OPENAI_API_KEY", None),
    ("deepgram_api_key", "WEBSEARCH_DEEPGRAM_API_KEY", "DEEPGRAM_API_KEY"),
    ("cartesia_api_key", "WEBSEARCH_CARTESIA_API_KEY", "CARTESIA_API_KEY"),
    ("cartesia_voice_id", "WEBSEARCH_CARTESIA_VOICE_ID", "CARTESIA_VOICE_ID"),
)


def _alias_families(
    runtime_named_key: str = _DEFAULT_OPENAI_KEY_ENV,
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    """The alias families in ``(field, members)`` form, one key per member.

    A function rather than a constant because a family's bare spelling can be
    named at runtime (``WEBSEARCH_OPENAI_API_KEY_ENV`` names the OpenAI one):
    passing that name in here keeps the runtime-named key inside the same
    registry every other family key is derived from, instead of assembling a
    one-off pair inline in ``load_config``. Which rows take it is declared in
    ``_ALIAS_FAMILY_SPELLINGS`` (a ``None`` bare key), not decided here.

    The member shape is ``(name, key)``, deliberately narrower than the
    endpoint families' ``(name, keys)``: :func:`_winning_alias_key` supports
    exactly one key per member, so the type it accepts now says so rather than
    advertising a shape it would crash on (round 8 confirm pass 5,
    Architecture Minor). :func:`_widen_members` adapts these to the shared
    shape for key enumeration.
    """
    return tuple(
        (
            field_name,
            (
                ("scoped", scoped_key),
                ("bare", runtime_named_key if bare_key is None else bare_key),
            ),
        )
        for field_name, scoped_key, bare_key in _ALIAS_FAMILY_SPELLINGS
    )


def _widen_members(
    members: Sequence[tuple[str, str]],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Adapt single-key alias members to the shared multi-key family shape."""
    return tuple((name, (key,)) for name, key in members)


def _families() -> tuple[tuple[str, tuple[tuple[str, tuple[str, ...]], ...]], ...]:
    """Every declared family, for callers that need the whole key roster.

    Key enumeration only -- the two kinds of family resolve by different
    rules (see :func:`_winning_alias_key`), so this is not a dispatch table.
    Both axes are now derived: the endpoint side splats
    ``_ENDPOINT_FAMILIES`` (which ``load_config`` also iterates) and the alias
    side splats ``_alias_families()`` (likewise), so neither can fall behind
    this registry. ``test_every_endpoint_member_constant_is_registered``
    additionally scans the module for a ``_*_ENDPOINT_MEMBERS`` constant that
    never reached ``_ENDPOINT_FAMILIES`` (round 8 confirm pass 5,
    Architecture Minor), and
    ``test_load_config_consumes_every_registered_endpoint_family`` pins that
    every registered row's builder is invoked and its result reaches the
    named Config field -- the half a naming-convention scan cannot see
    (round 9 confirm pass 6, Architecture Important).
    """
    return (
        *((field, members) for field, members, _build in _ENDPOINT_FAMILIES),
        *((field, _widen_members(members)) for field, members in _alias_families()),
    )


def _family_key_set(value: object) -> bool:
    """Whether a family member key carries a real value.

    Presence-and-non-emptiness, deliberately NOT :func:`_effectively_set`'s
    truthiness: a family key's value can be a typed TOML integer, and an
    explicit ``[tts] tts_ws_port = 0`` is a value the operator set -- one that
    must reach the range validator that names the field, not be silently
    treated as absent and replaced by the packaged default. Same
    truthiness-vs-membership bug class as ``max_citations = 0`` (round 6
    confirm pass 3, Logic Minor). ``""`` and whitespace-only stay absent: an
    empty override erases its own key without supplying an endpoint, leaving
    the family's other spellings to answer.
    """
    return value is not None and str(value).strip() != ""


def _family_keys(members: Sequence[tuple[str, tuple[str, ...]]]) -> tuple[str, ...]:
    """Every key named by ``members``, in declaration order.

    The single derivation of a family's key roster: nothing hand-maintains a
    second copy that a newly added spelling could be forgotten from (round 6
    confirm pass 3, Architecture Minor).
    """
    return tuple(key for _name, keys in members for key in keys)


def _family_layers(
    values: Mapping[str, object],
    layer_maps: Sequence[tuple[int, Mapping[str, object]]],
    keys: Sequence[str],
) -> dict[str, int]:
    """Highest precedence layer that set each of ``keys``, or ``-1``.

    Computed from the RAW per-layer mappings rather than from the merged
    ``layers`` provenance dict: family keys use :func:`_family_key_set` rather
    than ``_record_layer``'s stricter truthiness, so the provenance dict does
    not record them at all. (The OpenAI alias family's bare key name is not
    even known until resolution time -- ``WEBSEARCH_OPENAI_API_KEY_ENV`` names
    it -- so no static ``_PROVENANCE_KEYS`` roster could cover the families
    either way.)

    A key a higher layer *erased* (``WEBSEARCH_TTS_WS_PORT=""``) reports
    ``-1``, not the lower layer that last carried a value: family keys are
    deliberately outside the empty-means-absent rule, so the erase is a real
    write and the surviving merged value is empty. Reporting the overwritten
    layer would let a key that supplies nothing still decide which member
    wins.
    """
    return {
        key: (
            max(
                (layer for layer, mapping in layer_maps if _family_key_set(mapping.get(key))),
                default=-1,
            )
            if _family_key_set(values.get(key))
            else -1
        )
        for key in keys
    }


def _winning_family_member(
    values: Mapping[str, object],
    layers: Mapping[str, int],
    members: Sequence[tuple[str, tuple[str, ...]]],
) -> str | None:
    """Pick which member of a family supplies the setting.

    Resolves the ENDPOINT families -- the mutually-exclusive spellings of one
    STT/TTS endpoint (``WEBSEARCH_TTS_ENDPOINT`` / ``_WS_URI`` / ``_WS_SOCKET``
    / ``_WS_HOST`` + ``_WS_PORT``). Resolving them by hardcoded key priority
    ignored the layered precedence system entirely: a config.toml
    ``tts_ws_uri`` (layer 0) beat a ``WEBSEARCH_TTS_WS_SOCKET`` exported in the
    process environment (layer 2), so the operator's highest-precedence
    override was silently discarded and the service connected to the TOML
    endpoint (round-5 restart, Logic Important).

    The vendor-credential aliases are a family too but resolve through
    ``_winning_alias_key`` instead: every spelling here is ``WEBSEARCH_``-
    prefixed, and that safety property is what makes layer-first correct --
    the aliases do not have it.

    The winner is the present member set at the highest precedence layer; the
    declaration order of ``members`` (the documented key priority) breaks a
    same-layer tie, which is what preserves the README's "for TTS, URI takes
    precedence over socket, followed by host plus port" rule within one layer.
    A multi-key member (host + port) takes the highest layer among its keys.
    Returns ``None`` when no member is set, leaving the dataclass default.
    """
    present = [
        (name, max(layers.get(key, -1) for key in keys))
        for name, keys in members
        if all(_family_key_set(values.get(key)) for key in keys)
    ]
    if not present:
        return None
    return max(present, key=lambda member: member[1])[0]


def _winning_alias_key(
    values: Mapping[str, object],
    members: Sequence[tuple[str, str]],
) -> str | None:
    """Pick which spelling of a vendor credential supplies the value.

    Declaration order decides -- the scoped ``WEBSEARCH_``-prefixed spelling
    beats the bare one whatever layer either was set at -- and no layer is
    consulted at all: a member here is exactly one key, so there is no
    within-group tie to break. The parameter type says ``(name, key)`` rather
    than the endpoint families' ``(name, keys)`` for that reason; advertising
    the wider shape while unpacking a single key was a declared type this
    function could not honour (round 8 confirm pass 5, Architecture Minor).

    This is deliberately NOT :func:`_winning_family_member`'s layer-first rule,
    even though the two look like the same shape. The endpoint families can
    take that rule safely because every one of their spellings is
    ``WEBSEARCH_``-prefixed: nothing outside this app's own configuration can
    accidentally join the family. The credential aliases are the opposite by
    design -- their bare spellings (``OPENAI_API_KEY``, ``DEEPGRAM_API_KEY``,
    ``CARTESIA_*``) are the ubiquitous vendor names, offered precisely so an
    operator can reuse standard credentials. Under a layer-first rule an
    ambient ``OPENAI_API_KEY`` in a developer's shell -- never set for this app
    -- outranks the ``WEBSEARCH_OPENAI_API_KEY`` deliberately written into the
    project's env file, and the wrong credential leaves the process for a
    vendor API. Round 6 ported the layer-first rule here on the strength of
    the shape match; the ``WEBSEARCH_`` prefix exists to disambiguate FROM
    ambient vendor vars, so the scoped spelling must keep winning
    (round 7 confirm pass 4, Logic Minor).

    A caller can still put a bare credential on top: unset the scoped one.
    """
    for _name, key in members:
        if _family_key_set(values.get(key)):
            return key
    return None


def _winning_family_layer(
    layers: Mapping[str, int],
    members: Sequence[tuple[str, tuple[str, ...]]],
    winner: str | None,
) -> int:
    """The layer ``winner`` won at, or ``-1`` when no member won."""
    if winner is None:
        return -1
    keys = next(keys for name, keys in members if name == winner)
    return max(layers.get(key, -1) for key in keys)


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
    layers: dict[str, int] = {}
    # The raw per-layer mappings, kept alongside the merged `values` because
    # family resolution needs to know WHICH layer set each spelling -- see
    # _family_layers for why the merged `layers` provenance dict cannot answer
    # that for family keys.
    layer_maps: list[tuple[int, Mapping[str, object]]] = []
    toml_backed: dict[str, object] = {}
    _load_toml_values(toml_backed, toml_values)
    _apply_layer(values, toml_backed, layers, 0)
    layer_maps.append((0, toml_backed))
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
        layer_maps.append((1, env_file_values))
    env_values = os.environ if env is None else env
    _apply_layer(values, env_values, layers, 2)
    layer_maps.append((2, env_values))
    # Overriding only a role's *model* at a higher-precedence layer than the
    # one that set its *effort* must not silently carry that inherited
    # effort along -- see _clear_inherited_reasoning_effort's docstring.
    _clear_inherited_reasoning_effort(values, layers)
    kwargs: dict[str, object] = {}
    # The vendor-credential aliases resolve by declaration order (scoped beats
    # bare at any layer), not by the endpoint families' layer-first rule -- see
    # _winning_alias_key for why the two families cannot share that rule.
    key_name = str(values.get("WEBSEARCH_OPENAI_API_KEY_ENV") or _DEFAULT_OPENAI_KEY_ENV)
    for field_name, alias_members in _alias_families(key_name):
        alias_key = _winning_alias_key(values, alias_members)
        if alias_key is not None:
            kwargs[field_name] = values[alias_key]
    # Membership, not truthiness, and every conversion wrapped: TOML supplies
    # real ints, so an explicit `max_citations = 0` / `multi_intent_wait_timeout_ms
    # = 0` is falsy and a truthiness walrus dropped the key outright -- the
    # packaged default was substituted and `Config.__post_init__`'s field-specific
    # range check (which exists to reject exactly those values) never fired. Same
    # bug class the round-7 fix closed for the string fields below; the float
    # siblings at WEBSEARCH_SPEECH_* already use the corrected form.
    for env_name, field_name in (
        ("WEBSEARCH_MAX_WORK_ITEMS_PER_TURN", "max_work_items_per_turn"),
        ("WEBSEARCH_MULTI_INTENT_WAIT_TIMEOUT_MS", "multi_intent_wait_timeout_ms"),
        ("WEBSEARCH_MAX_CITATIONS", "max_citations"),
    ):
        if env_name in values:
            try:
                kwargs[field_name] = int(values[env_name])
            except (TypeError, ValueError) as exc:
                raise ConfigError(f"{env_name} must be an integer") from exc
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
    # Membership, not truthiness -- `__post_init__` rejects a zero/negative
    # smart-turn timeout by name, and a truthiness gate hid an explicit
    # `smart_turn_timeout_seconds = 0` behind the packaged default.
    if (raw := values.get("WEBSEARCH_SMART_TURN_TIMEOUT_SECONDS")) is not None:
        try:
            kwargs["smart_turn_timeout_seconds"] = float(raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError("WEBSEARCH_SMART_TURN_TIMEOUT_SECONDS must be a number") from exc
    if (raw := values.get("WEBSEARCH_SMART_TURN_COMPLETE_GRACE_SECONDS")) is not None:
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
    # `early_ack_text`/`release_version`, and the identity fields treat empty
    # as unbound. A walrus-truthiness check silently swallowed an
    # explicitly-empty operator setting and substituted the default, so the
    # validation could never fire. This matches the membership pattern the
    # boolean flags above already use.
    for env_name, field_name in (
        ("WEBSEARCH_EARLY_ACK_TEXT", "early_ack_text"),
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
    # Endpoint families resolve by LAYER precedence first, key priority second
    # -- see _winning_family_member. Key priority alone let `[stt] stt_ws_uri`
    # in config.toml (layer 0) beat `WEBSEARCH_STT_WS_SOCKET` exported in the
    # process environment (layer 2), silently connecting to the endpoint the
    # operator's highest-precedence override had replaced. This closes the gap
    # _clear_inherited_reasoning_effort's docstring signposted.
    #
    # Both RESOLVED and CONSUMED by iterating `_ENDPOINT_FAMILIES` rather than
    # naming the member constants here: pairing a family's key roster with its
    # layer computation is one drift-prone step, and reading the resolved
    # winner back out into `kwargs` is the other. A registry row resolved by
    # the loop but consumed by a hand-written `kwargs["..."]` branch was
    # silently dropped for any family whose branch nobody wrote (round 8
    # confirm pass 5 / round 9 confirm pass 6, Architecture).
    #
    # The members are STORED alongside the winner, not re-fetched from the
    # module constant: the half-pair guard below was the last consumer still
    # naming `_TTS_ENDPOINT_MEMBERS` directly, contradicting the registry
    # docstring (round 9 confirm pass 6, Architecture Minor).
    endpoint_resolution: dict[
        str, tuple[str | None, dict[str, int], tuple[tuple[str, tuple[str, ...]], ...]]
    ] = {}
    for family_name, family_members, _build in _ENDPOINT_FAMILIES:
        family_layers = _family_layers(values, layer_maps, _family_keys(family_members))
        endpoint_resolution[family_name] = (
            _winning_family_member(values, family_layers, family_members),
            family_layers,
            family_members,
        )
    tts_member, tts_layers, tts_members = endpoint_resolution["tts_endpoint"]
    # The pair's key names come from the registry row itself (the `host_port`
    # member of `_TTS_ENDPOINT_MEMBERS`), never from string literals here: the
    # guard cannot drift from the registry, and a row renamed, removed, or
    # regrown to a different arity fails loudly at THIS unpack -- there is no
    # silent-skip path (a `next(..., None)` default here previously let a
    # renamed row disable the guard with no error at all; only same-name
    # arity drift reached a failure).
    (tts_pair,) = (keys for name, keys in tts_members if name == "host_port")
    tts_host_key, tts_port_key = tts_pair
    tts_host = values.get(tts_host_key)
    tts_port = values.get(tts_port_key)
    if _family_key_set(tts_host) != _family_key_set(tts_port):
        # Half a pair is malformed input, and every other malformed endpoint
        # input here is loud (non-integer port, out-of-range port). Falling
        # through the chain silently substituted the dataclass default endpoint.
        #
        # Raised only when the half-pair would otherwise have WON the family --
        # i.e. it sits at a strictly higher layer than the winning member (a
        # same-layer stray loses to the documented key priority, exactly as a
        # complete pair would). Firing unconditionally turned a config that
        # booted fine into a startup failure: a leftover `[tts] tts_ws_host` in
        # config.toml is simply not consulted once `WEBSEARCH_TTS_ENDPOINT` is
        # exported in the environment, and never was (round 6 confirm pass 3,
        # Logic Important).
        half_pair_layer = max(tts_layers[tts_host_key], tts_layers[tts_port_key])
        if half_pair_layer > _winning_family_layer(tts_layers, tts_members, tts_member):
            raise ConfigError(f"{tts_host_key} and {tts_port_key} must be set together")
    for family_name, _family_members, build_endpoint in _ENDPOINT_FAMILIES:
        endpoint = build_endpoint(endpoint_resolution[family_name][0], values)
        if endpoint is not None:
            kwargs[family_name] = endpoint
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

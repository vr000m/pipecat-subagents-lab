"""Contract tests for operator configuration.

These tests deliberately exercise the configuration boundary rather than
environment-specific defaults.  They are test-first: the Phase 1
implementation is expected to provide ``Config`` and ``load_config``.
"""

from pathlib import Path

import pytest

from server.config import Config, ConfigError, FeaturePolicy, load_config


@pytest.fixture(autouse=True)
def _confine_manifest_evidence_root_to_tmp_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`load_promotion_manifest` confines manifest-declared `inputs[*].path`
    entries to the repo root; point that root at each test's own `tmp_path`
    so existing fixtures can keep writing dummy evidence files there while
    still exercising the real containment check (relative-path resolution,
    `is_relative_to`, `is_file`), not a bypass of it."""
    import server.config as _config_module

    monkeypatch.setattr(_config_module, "_REPO_ROOT", tmp_path)


def test_feature_switch_defaults_are_true() -> None:
    config = Config()

    assert config.enable_early_ack is True
    assert config.enable_background_status is True
    assert config.enable_autoplay_policy is True


def test_feature_policy_is_frozen_and_constructed_from_config() -> None:
    config = Config(
        enable_early_ack=False, enable_background_status=True, enable_autoplay_policy=False
    )

    policy = FeaturePolicy.from_config(config)

    assert policy.enable_early_ack is False
    assert policy.enable_background_status is True
    assert policy.enable_autoplay_policy is False
    with pytest.raises((AttributeError, TypeError)):
        policy.enable_early_ack = True  # type: ignore[misc]


def test_feature_switches_load_from_environment() -> None:
    config = load_config(
        env={
            "WEBSEARCH_ENABLE_EARLY_ACK": "false",
            "WEBSEARCH_ENABLE_BACKGROUND_STATUS": "false",
            "WEBSEARCH_ENABLE_AUTOPLAY_POLICY": "false",
        }
    )

    assert config.enable_early_ack is False
    assert config.enable_background_status is False
    assert config.enable_autoplay_policy is False


def test_feature_switches_load_from_toml_and_environment_wins(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        "[features]\n"
        "enable_early_ack = false\n"
        "enable_background_status = false\n"
        "enable_autoplay_policy = false\n"
    )

    from_toml = load_config(config_file=config_file, env={})
    from_environment = load_config(
        config_file=config_file,
        env={"WEBSEARCH_ENABLE_EARLY_ACK": "true"},
    )

    assert from_toml.enable_early_ack is False
    assert from_environment.enable_early_ack is True
    assert from_environment.enable_background_status is False
    assert from_environment.enable_autoplay_policy is False


def test_feature_switch_invalid_boolean_value_has_config_error_boundary() -> None:
    with pytest.raises(ConfigError, match="WEBSEARCH_ENABLE_EARLY_ACK"):
        load_config(env={"WEBSEARCH_ENABLE_EARLY_ACK": "not-a-boolean"})


def test_early_ack_text_override_defaults_to_a_non_progress_claiming_fixed_wording() -> None:
    config = Config()

    assert isinstance(config.early_ack_text, str)
    assert config.early_ack_text.strip() != ""
    lowered = config.early_ack_text.lower()
    for claim in ("found", "here is", "here's", "result"):
        assert claim not in lowered

    overridden = load_config(env={"WEBSEARCH_EARLY_ACK_TEXT": "One moment."})
    assert overridden.early_ack_text == "One moment."


def test_defaults_are_bounded_and_do_not_contain_credentials() -> None:
    config = Config()

    assert config.bind_host == "127.0.0.1"
    assert config.bind_port == 7860
    assert config.known_client_url == "http://127.0.0.1:7860"
    assert config.max_work_items_per_turn == 2
    assert config.multi_intent_wait_timeout_ms == 10_000
    assert config.foreground_search_timeout_seconds == 15.0
    assert config.router_timeout_seconds == 12.0
    assert config.provider_timeout_seconds == 75.0
    assert config.shutdown_grace_seconds == 2.0
    assert config.max_citations == 12
    assert config.pending_dialogue_timeout_seconds == 30.0
    assert config.stt_service == "websocket"
    assert config.stt_provider == "local"
    assert config.stt_model == "nova-3-general"
    assert config.stt_language == "en"
    assert config.smart_turn_timeout_seconds == 5.0
    assert config.smart_turn_complete_grace_seconds == 1.5
    assert config.tts_voice_id == "azelma"
    assert config.tts_provider == "local"
    assert config.tts_model == "sonic-3.5"
    assert config.openai_api_key is None
    assert config.router_model_policy == {"fast": "gpt-5-mini"}
    assert config.worker_model_policy == {"deep": "gpt-5"}


def test_operator_limits_reject_zero_or_unbounded_values() -> None:
    with pytest.raises(ConfigError):
        Config(max_work_items_per_turn=0)

    with pytest.raises(ConfigError):
        Config(max_work_items_per_turn=5)

    with pytest.raises(ConfigError):
        Config(multi_intent_wait_timeout_ms=0)

    with pytest.raises(ConfigError):
        Config(foreground_search_timeout_seconds=0)

    with pytest.raises(ConfigError):
        Config(provider_timeout_seconds=15, foreground_search_timeout_seconds=15)

    with pytest.raises(ConfigError):
        Config(router_timeout_seconds=76)

    with pytest.raises(ConfigError):
        Config(shutdown_grace_seconds=float("inf"))

    with pytest.raises(ConfigError):
        Config(max_citations=0)

    with pytest.raises(ConfigError):
        Config(pending_dialogue_timeout_seconds=0)

    with pytest.raises(ConfigError):
        Config(pending_dialogue_timeout_seconds=float("nan"))

    with pytest.raises(ConfigError):
        Config(pending_dialogue_timeout_seconds=float("inf"))

    with pytest.raises(ConfigError):
        Config(smart_turn_timeout_seconds=0)

    with pytest.raises(ConfigError):
        Config(smart_turn_timeout_seconds=61)

    with pytest.raises(ConfigError):
        Config(smart_turn_complete_grace_seconds=0)

    with pytest.raises(ConfigError, match="stt_provider"):
        Config(stt_provider="unknown")

    with pytest.raises(ConfigError, match="tts_provider"):
        Config(tts_provider="unknown")


def test_env_precedence_is_explicit_and_secret_values_are_not_logged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WEBSEARCH_MAX_WORK_ITEMS_PER_TURN", "4")
    monkeypatch.setenv("WEBSEARCH_OPENAI_API_KEY", "test-secret-value")

    config = load_config(
        env_file={
            "WEBSEARCH_MAX_WORK_ITEMS_PER_TURN": "3",
            "WEBSEARCH_OPENAI_API_KEY": "file-secret-value",
        }
    )

    assert config.max_work_items_per_turn == 4
    assert config.openai_api_key == "test-secret-value"
    assert "test-secret-value" not in repr(config)
    assert "file-secret-value" not in repr(config)


def test_model_selection_uses_policy_labels_not_model_ids_from_untrusted_input() -> None:
    config = Config(
        router_model_policy={"fast": "verified-router-model"},
        worker_model_policy={"deep": "verified-worker-model"},
    )

    assert config.resolve_router_model("fast") == "verified-router-model"
    with pytest.raises(ConfigError):
        config.resolve_router_model("model-emitted-id")


def test_operator_models_load_from_toml_and_environment_wins(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text('[models]\nrouter_model = "toml-router"\nworker_model = "toml-worker"\n')

    config = load_config(
        config_file=config_file,
        env={"WEBSEARCH_ROUTER_MODEL": "env-router"},
    )

    assert config.resolve_router_model("fast") == "env-router"
    assert config.resolve_worker_model("deep") == "toml-worker"


def test_bind_and_known_client_settings_load_from_environment() -> None:
    config = load_config(
        env={
            "WEBSEARCH_BIND_HOST": "127.0.0.2",
            "WEBSEARCH_BIND_PORT": "9000",
            "WEBSEARCH_KNOWN_CLIENT_URL": "https://example.test:9443/client",
        }
    )

    assert (config.bind_host, config.bind_port, config.known_client_url) == (
        "127.0.0.2",
        9000,
        "https://example.test:9443/client",
    )


def test_smart_turn_timeout_loads_from_environment() -> None:
    config = load_config(env={"WEBSEARCH_SMART_TURN_TIMEOUT_SECONDS": "7.5"})

    assert config.smart_turn_timeout_seconds == 7.5


def test_smart_turn_complete_grace_loads_from_environment() -> None:
    config = load_config(env={"WEBSEARCH_SMART_TURN_COMPLETE_GRACE_SECONDS": "2.25"})

    assert config.smart_turn_complete_grace_seconds == 2.25


def test_pending_dialogue_timeout_loads_from_environment() -> None:
    config = load_config(env={"WEBSEARCH_PENDING_DIALOGUE_TIMEOUT_SECONDS": "45"})

    assert config.pending_dialogue_timeout_seconds == 45.0


def test_search_lifecycle_limits_load_from_toml_and_environment_wins(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        "[turn]\n"
        "foreground_search_timeout_seconds = 20\n"
        "router_timeout_seconds = 9\n"
        "provider_timeout_seconds = 80\n"
        "shutdown_grace_seconds = 3\n"
        "max_citations = 9\n"
    )

    config = load_config(
        config_file=config_file,
        env={"WEBSEARCH_FOREGROUND_SEARCH_TIMEOUT_SECONDS": "25"},
    )

    assert config.foreground_search_timeout_seconds == 25
    assert config.router_timeout_seconds == 9
    assert config.provider_timeout_seconds == 80
    assert config.shutdown_grace_seconds == 3
    assert config.max_citations == 9


def test_invalid_pending_dialogue_timeout_has_config_error_boundary() -> None:
    with pytest.raises(ConfigError, match="WEBSEARCH_PENDING_DIALOGUE_TIMEOUT_SECONDS"):
        load_config(env={"WEBSEARCH_PENDING_DIALOGUE_TIMEOUT_SECONDS": "not-a-number"})


def test_invalid_smart_turn_timeout_has_config_error_boundary() -> None:
    with pytest.raises(ConfigError, match="WEBSEARCH_SMART_TURN_TIMEOUT_SECONDS"):
        load_config(env={"WEBSEARCH_SMART_TURN_TIMEOUT_SECONDS": "not-a-number"})


def test_invalid_smart_turn_complete_grace_has_config_error_boundary() -> None:
    with pytest.raises(ConfigError, match="WEBSEARCH_SMART_TURN_COMPLETE_GRACE_SECONDS"):
        load_config(env={"WEBSEARCH_SMART_TURN_COMPLETE_GRACE_SECONDS": "not-a-number"})


def test_smart_turn_environment_timeout_overrides_toml(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text("[turn]\nsmart_turn_timeout_seconds = 8.0\n")

    config = load_config(
        config_file=config_file,
        env={"WEBSEARCH_SMART_TURN_TIMEOUT_SECONDS": "6.5"},
    )

    assert config.smart_turn_timeout_seconds == 6.5


def test_pending_dialogue_timeout_loads_from_toml_and_environment_wins(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text("[turn]\npending_dialogue_timeout_seconds = 40.0\n")

    from_toml = load_config(config_file=config_file, env={})
    from_environment = load_config(
        config_file=config_file,
        env={"WEBSEARCH_PENDING_DIALOGUE_TIMEOUT_SECONDS": "45"},
    )

    assert from_toml.pending_dialogue_timeout_seconds == 40.0
    assert from_environment.pending_dialogue_timeout_seconds == 45.0


def test_invalid_pending_dialogue_timeout_from_toml_is_not_ignored(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text("[turn]\npending_dialogue_timeout_seconds = 0\n")

    with pytest.raises(ConfigError, match="pending_dialogue_timeout_seconds"):
        load_config(config_file=config_file, env={})


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("bind_host", ""),
        ("bind_host", "0.0.0.0"),
        ("bind_host", "192.168.1.20"),
        ("bind_port", 0),
        ("bind_port", 65_536),
        ("known_client_url", "client"),
    ),
)
def test_bind_settings_are_validated(field: str, value: object) -> None:
    with pytest.raises(ConfigError):
        Config(**{field: value})


@pytest.mark.parametrize("host", ("localhost", "127.0.0.1", "127.0.0.2", "::1"))
def test_loopback_bind_hosts_are_allowed(host: str) -> None:
    assert Config(bind_host=host).bind_host == host


def test_bind_port_environment_value_has_config_error_boundary() -> None:
    with pytest.raises(ConfigError, match="WEBSEARCH_BIND_PORT"):
        load_config(env={"WEBSEARCH_BIND_PORT": "not-a-port"})


def test_empty_bind_environment_values_are_not_silently_ignored() -> None:
    with pytest.raises(ConfigError):
        load_config(env={"WEBSEARCH_BIND_HOST": ""})


def test_toml_local_service_settings_load_and_expand_socket_paths(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        '[stt]\nstt_service = "websocket"\nstt_ws_socket = "~/stt.sock"\n'
        'stt_language = "en-US"\n[turn]\nsmart_turn_timeout_seconds = 8.0\n'
        "smart_turn_complete_grace_seconds = 2.0\n"
        '[tts]\ntts_ws_socket = "~/tts.sock"\n'
    )

    config = load_config(config_file=config_file, env={})

    assert config.stt_service == "websocket"
    assert config.stt_language == "en-US"
    assert config.stt_endpoint == ("uds", str(Path.home() / "stt.sock"))
    assert config.smart_turn_timeout_seconds == 8.0
    assert config.smart_turn_complete_grace_seconds == 2.0
    assert config.tts_endpoint == ("uds", str(Path.home() / "tts.sock"))


def test_hosted_provider_models_load_from_toml_and_environment_wins(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        '[stt]\nprovider = "deepgram"\nmodel = "nova-3-medical"\n'
        '[tts]\nprovider = "cartesia"\nmodel = "sonic-3"\n'
    )

    config = load_config(
        config_file=config_file,
        env={
            "DEEPGRAM_API_KEY": "deepgram-secret",
            "CARTESIA_API_KEY": "cartesia-secret",
            "CARTESIA_VOICE_ID": "voice-uuid",
            "WEBSEARCH_TTS_MODEL": "sonic-3.5",
        },
    )

    assert (config.stt_provider, config.stt_model) == ("deepgram", "nova-3-medical")
    assert (config.tts_provider, config.tts_model) == ("cartesia", "sonic-3.5")
    assert config.cartesia_voice_id == "voice-uuid"
    assert "deepgram-secret" not in repr(config)
    assert "cartesia-secret" not in repr(config)
    assert "voice-uuid" not in repr(config)


def test_repository_config_contains_the_local_socket_defaults() -> None:
    config = load_config(config_file=Path(__file__).parents[1] / "config.toml", env={})

    assert config.stt_endpoint == (
        "uds",
        str(Path.home() / "Library/Caches/pipecat-stt/nemotron.sock"),
    )
    assert config.tts_endpoint == (
        "ws",
        "127.0.0.1:8965",
    )
    assert config.tts_voice_id == "azelma"
    assert config.stt_provider == "local"
    assert config.tts_provider == "local"
    assert config.smart_turn_timeout_seconds == 5.0
    assert config.smart_turn_complete_grace_seconds == 1.5
    assert config.resolve_router_model("fast") == "gpt-5-mini"
    assert config.resolve_worker_model("deep") == "gpt-5"


def test_environment_endpoint_overrides_toml_socket(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text('[stt]\nstt_ws_socket = "~/from-toml.sock"\n')

    config = load_config(
        config_file=config_file,
        env={"WEBSEARCH_STT_ENDPOINT": "uds:///tmp/from-env.sock"},
    )

    assert config.stt_endpoint == ("uds", "/tmp/from-env.sock")


def test_speech_liveness_timeout_defaults() -> None:
    config = Config()

    assert config.speech_start_timeout_seconds == 10.0
    assert config.speech_transport_grace_seconds == 1.0


def test_speech_liveness_timeouts_reject_zero_or_non_finite_values() -> None:
    with pytest.raises(ConfigError):
        Config(speech_start_timeout_seconds=0)

    with pytest.raises(ConfigError):
        Config(speech_start_timeout_seconds=float("inf"))

    with pytest.raises(ConfigError):
        Config(speech_transport_grace_seconds=0)

    with pytest.raises(ConfigError):
        Config(speech_transport_grace_seconds=float("nan"))


def test_speech_liveness_timeouts_load_from_environment() -> None:
    config = load_config(
        env={
            "WEBSEARCH_SPEECH_START_TIMEOUT_SECONDS": "12.5",
            "WEBSEARCH_SPEECH_TRANSPORT_GRACE_SECONDS": "2.5",
        }
    )

    assert config.speech_start_timeout_seconds == 12.5
    assert config.speech_transport_grace_seconds == 2.5


def test_speech_liveness_timeouts_load_from_toml_and_environment_wins(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        "[turn]\nspeech_start_timeout_seconds = 8.0\nspeech_transport_grace_seconds = 0.5\n"
    )

    from_toml = load_config(config_file=config_file, env={})
    from_environment = load_config(
        config_file=config_file,
        env={"WEBSEARCH_SPEECH_START_TIMEOUT_SECONDS": "9.0"},
    )

    assert from_toml.speech_start_timeout_seconds == 8.0
    assert from_toml.speech_transport_grace_seconds == 0.5
    assert from_environment.speech_start_timeout_seconds == 9.0
    assert from_environment.speech_transport_grace_seconds == 0.5


def test_invalid_speech_start_timeout_environment_value_has_config_error_boundary() -> None:
    with pytest.raises(ConfigError, match="WEBSEARCH_SPEECH_START_TIMEOUT_SECONDS"):
        load_config(env={"WEBSEARCH_SPEECH_START_TIMEOUT_SECONDS": "not-a-number"})


def test_invalid_speech_transport_grace_environment_value_has_config_error_boundary() -> None:
    with pytest.raises(ConfigError, match="WEBSEARCH_SPEECH_TRANSPORT_GRACE_SECONDS"):
        load_config(env={"WEBSEARCH_SPEECH_TRANSPORT_GRACE_SECONDS": "not-a-number"})


def test_zero_speech_liveness_timeout_from_toml_is_not_silently_ignored(tmp_path) -> None:
    """A legitimate-looking ``0.0`` in TOML must not be treated the same as
    "absent" -- it is truthy-falsy in Python but a real override attempt, and
    must surface as a validation error rather than silently falling back to
    the default with no error at all."""
    config_file = tmp_path / "config.toml"
    config_file.write_text("[turn]\nspeech_start_timeout_seconds = 0.0\n")

    with pytest.raises(ConfigError, match="speech_start_timeout_seconds"):
        load_config(config_file=config_file, env={})


def test_zero_speech_transport_grace_from_toml_is_not_silently_ignored(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text("[turn]\nspeech_transport_grace_seconds = 0.0\n")

    with pytest.raises(ConfigError, match="speech_transport_grace_seconds"):
        load_config(config_file=config_file, env={})


def test_repository_config_declares_speech_liveness_timeouts() -> None:
    config = load_config(config_file=Path(__file__).parents[1] / "config.toml", env={})

    assert config.speech_start_timeout_seconds == 10.0
    assert config.speech_transport_grace_seconds == 1.0


def test_tts_uri_precedes_socket_and_host_port(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        '[tts]\ntts_ws_uri = "wss://tts.example.test:9443"\n'
        'tts_ws_socket = "~/tts.sock"\ntts_ws_host = "ignored.example.test"\ntts_ws_port = 9000\n'
    )

    config = load_config(config_file=config_file, env={})

    assert config.tts_endpoint == ("wss", "tts.example.test:9443")


def test_feature_policy_precedence_matrix_disabling_early_ack_forces_legacy_timeout_regardless_of_background_status() -> (
    None
):
    """Rollout safety matrix (dev plan Integration Seams): disabling
    ``enable_early_ack`` always forces the legacy timeout path even when
    ``enable_background_status`` remains on."""
    always_off_ack = FeaturePolicy.from_config(
        Config(enable_early_ack=False, enable_background_status=True)
    )
    also_off_ack = FeaturePolicy.from_config(
        Config(enable_early_ack=False, enable_background_status=False)
    )

    assert always_off_ack.enable_early_ack is False
    assert also_off_ack.enable_early_ack is False


def test_feature_policy_disabling_background_status_does_not_disable_ack_or_autoplay() -> None:
    policy = FeaturePolicy.from_config(
        Config(enable_early_ack=True, enable_background_status=False, enable_autoplay_policy=True)
    )

    assert policy.enable_early_ack is True
    assert policy.enable_autoplay_policy is True
    assert policy.enable_background_status is False


# -- Phase 2: promotion-manifest loader (server/config.py's
# load_promotion_manifest()) -----------------------------------------------
#
# Plan bullets 183, 190-192: a missing, unreadable, schema-invalid, stale,
# source-mismatched, policy-fingerprint-mismatched, wrong-phase (provisional
# without a Phase 3 override), or promotion_eligible=false manifest at
# whatever path is configured must fail closed to display-only rather than
# raise or block server startup. Only a manifest_phase="final" manifest with
# promotion_eligible=true, matching source/tree-hash/policy-fingerprint
# identity, and a generated_at_utc at or after Config.deployed_at_utc may
# report promotion-eligible.


def test_config_promotion_manifest_path_defaults_under_docs_benchmarks() -> None:
    config = Config()
    assert str(config.promotion_manifest_path) == "docs/benchmarks/v0.1.3-promotion-manifest.json"


def test_config_promotion_manifest_path_is_overridable_for_packaged_deployments() -> None:
    config = Config(promotion_manifest_path="/opt/app/promotion-manifest.json")
    assert str(config.promotion_manifest_path) == "/opt/app/promotion-manifest.json"


def _evidence_schema_hash() -> str:
    """The digest `validate_v013_evidence.py --write-manifest` stamps.

    The loader now recomputes it, so a fixture manifest must carry the real
    value rather than a placeholder.
    """
    import hashlib
    from pathlib import Path as _Path

    schema = _Path(__file__).resolve().parent.parent / "shared/schemas/v013-evidence.json"
    return hashlib.sha256(schema.read_bytes()).hexdigest()


def _write_dummy_evidence_file(tmp_path: Path, name: str, content: bytes) -> dict[str, str]:
    """A real file with a real digest, so the loader's byte-verification of
    `inputs[phase]` (mirroring the Phase 4C byte-check) can actually resolve
    it -- a placeholder path/hash pair no longer suffices."""
    import hashlib

    path = tmp_path / name
    if not path.exists():
        path.write_bytes(content)
    # Relative to the repo root the loader confines evidence reads to (this
    # test module's `tmp_path`, via the autouse `_REPO_ROOT` monkeypatch).
    return {"path": name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def _final_manifest(*, tmp_path: Path, config: Config | None = None, **overrides: object) -> dict:
    from server.config import FeaturePolicy, feature_policy_fingerprint

    resolved_config = config or Config()
    inputs = {
        "phase0": _write_dummy_evidence_file(tmp_path, "phase0.jsonl", b"phase0-evidence\n"),
        "phase1": _write_dummy_evidence_file(tmp_path, "phase1.jsonl", b"phase1-evidence\n"),
        "phase2": _write_dummy_evidence_file(tmp_path, "phase2.json", b"phase2-evidence\n"),
        "phase3": _write_dummy_evidence_file(tmp_path, "phase3.json", b"phase3-evidence\n"),
    }
    manifest = {
        "manifest_phase": "final",
        "promotion_eligible": True,
        "reason": None,
        "schema_hash": _evidence_schema_hash(),
        "release_version": resolved_config.release_version,
        "source_commit": "a" * 40,
        "source_tree_hash": "b" * 64,
        "feature_policy_fingerprint": feature_policy_fingerprint(
            FeaturePolicy.from_config(resolved_config)
        ),
        "deployed_at_utc": "2026-08-04T00:00:00Z",
        "generated_at_utc": "2026-08-04T00:05:00Z",
        # The Phase 3 input binding and the top-level completion hash are two
        # records of the same fact; the loader requires them to agree.
        "phase3_completion_hash": inputs["phase3"]["sha256"],
        "inputs": inputs,
    }
    manifest.update(overrides)
    return manifest


def _load_promotion_manifest():
    try:
        from server.config import load_promotion_manifest
    except ImportError:
        pytest.skip("load_promotion_manifest not yet implemented (Phase 2 concurrent implementer)")
    return load_promotion_manifest


def test_load_promotion_manifest_missing_file_fails_closed_to_display_only(tmp_path: Path) -> None:
    load_promotion_manifest = _load_promotion_manifest()
    config = Config(promotion_manifest_path=str(tmp_path / "missing.json"))

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "manifest_missing"


def test_load_promotion_manifest_schema_invalid_json_fails_closed(tmp_path: Path) -> None:
    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{not valid json", encoding="utf-8")
    config = Config(promotion_manifest_path=str(manifest_path))

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "manifest_malformed"


def test_load_promotion_manifest_missing_required_field_is_schema_invalid(tmp_path: Path) -> None:
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    incomplete = _final_manifest(tmp_path=tmp_path)
    del incomplete["schema_hash"]
    manifest_path.write_text(json.dumps(incomplete), encoding="utf-8")
    config = Config(promotion_manifest_path=str(manifest_path))

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "manifest_schema_invalid"


def test_load_promotion_manifest_provisional_phase_always_fails_closed(tmp_path: Path) -> None:
    """Plan: 'A provisional manifest is accepted for diagnostics but is
    permanently promotion_eligible=false and display-only.'"""
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            _final_manifest(
                tmp_path=tmp_path, manifest_phase="provisional", promotion_eligible=True
            )
        ),
        encoding="utf-8",
    )
    config = Config(promotion_manifest_path=str(manifest_path))

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "provisional_manifest"


def test_load_promotion_manifest_wrong_phase_final_without_phase3_completion_is_incomplete(
    tmp_path: Path,
) -> None:
    """Plan: Phase 2 does not implement Phase 3's completion stamping, so a
    'final' manifest seen before Phase 3 lands must not silently pass."""
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    incomplete = _final_manifest(tmp_path=tmp_path)
    del incomplete["phase3_completion_hash"]
    manifest_path.write_text(json.dumps(incomplete), encoding="utf-8")
    config = Config(promotion_manifest_path=str(manifest_path))

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "incomplete_final_manifest"


def test_load_promotion_manifest_source_commit_mismatch_fails_closed(tmp_path: Path) -> None:
    import json

    load_promotion_manifest = _load_promotion_manifest()
    config = Config(
        promotion_manifest_path=str(tmp_path / "manifest.json"),
        source_commit="f" * 40,
        source_tree_hash="b" * 64,
        deployed_at_utc="2026-08-01T00:00:00Z",
    )
    Path(config.promotion_manifest_path).write_text(
        json.dumps(_final_manifest(tmp_path=tmp_path, config=config, source_commit="a" * 40)),
        encoding="utf-8",
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "source_mismatch"


def test_load_promotion_manifest_policy_fingerprint_mismatch_fails_closed(tmp_path: Path) -> None:
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    # Fingerprinted against the default (all-on) FeaturePolicy, then loaded
    # with a Config whose FeaturePolicy differs -- a stale binding.
    manifest_path.write_text(json.dumps(_final_manifest(tmp_path=tmp_path)), encoding="utf-8")
    config = Config(
        promotion_manifest_path=str(manifest_path),
        enable_autoplay_policy=False,
        deployed_at_utc="2026-08-01T00:00:00Z",
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "policy_fingerprint_mismatch"


def test_load_promotion_manifest_stale_generated_before_deployed_fails_closed(
    tmp_path: Path,
) -> None:
    """Plan: 'a manifest is stale when ... its generated_at_utc predates
    Config.deployed_at_utc.'"""
    import json

    load_promotion_manifest = _load_promotion_manifest()
    config = Config(
        promotion_manifest_path=str(tmp_path / "manifest.json"),
        source_commit="a" * 40,
        source_tree_hash="b" * 64,
        deployed_at_utc="2026-08-04T00:00:00Z",
    )
    Path(config.promotion_manifest_path).write_text(
        json.dumps(
            _final_manifest(
                tmp_path=tmp_path, config=config, generated_at_utc="2026-08-01T00:00:00Z"
            )
        ),
        encoding="utf-8",
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "stale"


def test_load_promotion_manifest_promotion_eligible_false_stays_display_only(
    tmp_path: Path,
) -> None:
    import json

    load_promotion_manifest = _load_promotion_manifest()
    config = Config(
        promotion_manifest_path=str(tmp_path / "manifest.json"),
        source_commit="a" * 40,
        source_tree_hash="b" * 64,
        deployed_at_utc="2026-08-01T00:00:00Z",
    )
    Path(config.promotion_manifest_path).write_text(
        json.dumps(
            _final_manifest(
                tmp_path=tmp_path,
                config=config,
                promotion_eligible=False,
                reason="real_stratum_missing",
            )
        ),
        encoding="utf-8",
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "real_stratum_missing"


def test_load_promotion_manifest_valid_matching_final_manifest_is_promotion_eligible(
    tmp_path: Path,
) -> None:
    import json

    load_promotion_manifest = _load_promotion_manifest()
    config = Config(
        promotion_manifest_path=str(tmp_path / "manifest.json"),
        source_commit="a" * 40,
        source_tree_hash="b" * 64,
        deployed_at_utc="2026-08-01T00:00:00Z",
    )
    Path(config.promotion_manifest_path).write_text(
        json.dumps(_final_manifest(tmp_path=tmp_path, config=config)), encoding="utf-8"
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is True


def test_load_promotion_manifest_rejects_an_absolute_phase_input_path(tmp_path: Path) -> None:
    """Regression: a manifest-declared `inputs[*].path` is attacker-steerable
    (it lives inside the artifact under validation, not operator config), so
    an absolute path must never be accepted as a runtime read target -- e.g.
    `/etc/shadow` -- even though its digest would legitimately fail to match."""
    import hashlib
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    manifest = _final_manifest(tmp_path=tmp_path, config=config)
    outside_secret = tmp_path.parent / "outside-secret.txt"
    outside_secret.write_bytes(b"do-not-read-me\n")
    manifest["inputs"]["phase0"] = {
        "path": str(outside_secret),
        "sha256": hashlib.sha256(outside_secret.read_bytes()).hexdigest(),
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "evidence_unresolvable"


def test_load_promotion_manifest_rejects_an_oversized_phase_input_path(tmp_path: Path) -> None:
    """Regression: `_MAX_EVIDENCE_INPUT_BYTES` is declared alongside
    `_resolve_confined_evidence_path` as part of the same rationale, but the
    helper never used to consult it -- the cap was applied separately at the
    call site, so a second caller would inherit path confinement but
    silently not the read-size cap. The cap now lives inside the helper
    itself so every caller inherits both together."""
    import hashlib
    import json

    from server import config as _config_module

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    manifest = _final_manifest(tmp_path=tmp_path, config=config)
    oversized = tmp_path / "oversized-phase0.jsonl"
    oversized.write_bytes(b"x" * (_config_module._MAX_EVIDENCE_INPUT_BYTES + 1))
    manifest["inputs"]["phase0"] = {
        "path": oversized.name,
        "sha256": hashlib.sha256(oversized.read_bytes()).hexdigest(),
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "evidence_unresolvable"


def test_load_promotion_manifest_rejects_a_traversal_phase_input_path(tmp_path: Path) -> None:
    """Regression: a relative `inputs[*].path` containing `../` must not be
    allowed to escape the confined repo root after resolution."""
    import hashlib
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    manifest = _final_manifest(tmp_path=tmp_path, config=config)
    outside_secret = tmp_path.parent / "outside-secret.txt"
    outside_secret.write_bytes(b"do-not-read-me\n")
    manifest["inputs"]["phase0"] = {
        "path": f"../{outside_secret.name}",
        "sha256": hashlib.sha256(outside_secret.read_bytes()).hexdigest(),
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "evidence_unresolvable"


def test_load_promotion_manifest_missing_or_malformed_metadata_never_blocks_server_startup(
    tmp_path: Path,
) -> None:
    """Plan (Architecture Decisions): 'Fail-closed, not fail-fast: missing or
    malformed metadata never prevents server boot.' Constructing Config and
    calling the loader with no deployment identity set must not raise."""
    load_promotion_manifest = _load_promotion_manifest()
    config = Config(promotion_manifest_path=str(tmp_path / "missing.json"))

    verdict = load_promotion_manifest(config)  # must not raise
    assert verdict.promotion_eligible is False


# -- Config hardening: manifest type validation, timestamp parsing, the
# phase4c artifact path loader, and FeaturePolicy value semantics ----------


def _bound_config(manifest_path: Path, **overrides: object) -> Config:
    """A Config whose deployment identity is fully bound to `_final_manifest`."""
    kwargs: dict[str, object] = {
        "promotion_manifest_path": str(manifest_path),
        "source_commit": "a" * 40,
        "source_tree_hash": "b" * 64,
        "deployed_at_utc": "2026-08-01T00:00:00Z",
    }
    kwargs.update(overrides)
    return Config(**kwargs)  # type: ignore[arg-type]


def test_load_promotion_manifest_non_string_timestamp_fails_closed_without_raising(
    tmp_path: Path,
) -> None:
    """A non-str JSON value where a timestamp is expected must degrade to a
    display-only verdict, never raise out of the loader and abort boot."""
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    manifest_path.write_text(
        json.dumps(_final_manifest(tmp_path=tmp_path, config=config, generated_at_utc=12345)),
        encoding="utf-8",
    )

    verdict = load_promotion_manifest(config)  # must not raise

    assert verdict.promotion_eligible is False
    assert verdict.reason


def test_load_promotion_manifest_rejects_non_dict_inputs(tmp_path: Path) -> None:
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    manifest_path.write_text(
        json.dumps(_final_manifest(tmp_path=tmp_path, config=config, inputs="not-a-mapping")),
        encoding="utf-8",
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "manifest_schema_invalid"


def test_load_promotion_manifest_rejects_a_non_hex_schema_hash(tmp_path: Path) -> None:
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    manifest_path.write_text(
        json.dumps(_final_manifest(tmp_path=tmp_path, config=config, schema_hash="not-a-hash")),
        encoding="utf-8",
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "manifest_schema_invalid"


def test_load_promotion_manifest_rejects_a_non_hex_phase3_completion_hash(tmp_path: Path) -> None:
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    manifest_path.write_text(
        json.dumps(_final_manifest(tmp_path=tmp_path, config=config, phase3_completion_hash="yes")),
        encoding="utf-8",
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False


def test_load_promotion_manifest_unset_source_identity_is_unbound_not_skipped(
    tmp_path: Path,
) -> None:
    """An unset `Config.source_commit` means the runtime cannot prove the
    manifest describes *this* build -- that is unbound, not "skip the check"."""
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = Config(
        promotion_manifest_path=str(manifest_path),
        source_tree_hash="b" * 64,
        deployed_at_utc="2026-08-01T00:00:00Z",
    )
    manifest_path.write_text(
        json.dumps(_final_manifest(tmp_path=tmp_path, config=config)), encoding="utf-8"
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "identity_unbound"


def test_load_promotion_manifest_compares_timestamps_as_instants_not_strings(
    tmp_path: Path,
) -> None:
    """`Z` and `+00:00` denote the same instant; a naive string compare does
    not, and would mark an equal-instant manifest stale."""
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"

    config = _bound_config(manifest_path, deployed_at_utc="2026-01-01T00:00:00+00:00")
    manifest_path.write_text(
        json.dumps(
            _final_manifest(
                tmp_path=tmp_path, config=config, generated_at_utc="2026-01-01T00:00:00Z"
            )
        ),
        encoding="utf-8",
    )
    assert load_promotion_manifest(config).promotion_eligible is True

    config = _bound_config(manifest_path, deployed_at_utc="2026-01-01T00:00:00Z")
    manifest_path.write_text(
        json.dumps(
            _final_manifest(
                tmp_path=tmp_path, config=config, generated_at_utc="2026-01-01T00:00:00+00:00"
            )
        ),
        encoding="utf-8",
    )
    assert load_promotion_manifest(config).promotion_eligible is True


def test_phase4c_artifact_path_loads_from_environment() -> None:
    config = load_config(env={"WEBSEARCH_PHASE4C_ARTIFACT_PATH": "docs/benchmarks/phase4c.json"})

    assert config.phase4c_artifact_path == "docs/benchmarks/phase4c.json"


def test_phase4c_artifact_path_loads_from_toml_and_environment_wins(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        '[features]\nphase4c_artifact_path = "docs/benchmarks/from-toml.json"\n', encoding="utf-8"
    )

    from_toml = load_config(env={}, config_file=config_file)
    assert from_toml.phase4c_artifact_path == "docs/benchmarks/from-toml.json"

    from_env = load_config(
        env={"WEBSEARCH_PHASE4C_ARTIFACT_PATH": "/opt/app/phase4c.json"}, config_file=config_file
    )
    assert from_env.phase4c_artifact_path == "/opt/app/phase4c.json"


def test_load_promotion_manifest_without_a_phase4c_path_is_unresolvable(tmp_path: Path) -> None:
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    assert config.phase4c_artifact_path is None
    manifest_path.write_text(
        json.dumps(
            _final_manifest(tmp_path=tmp_path, config=config, phase4c_artifact_sha256="f" * 64)
        ),
        encoding="utf-8",
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "phase4c_unresolvable"


def test_load_promotion_manifest_rejects_an_oversized_phase4c_artifact_path(tmp_path: Path) -> None:
    """Regression: `phase4c_artifact_path` (operator config -- env var or
    TOML `[features]` table, never manifest-declared) was read via a bare
    `read_bytes()` with no `is_file()`/size check, unlike the sibling
    phase0-3 `inputs[*].path` entries `_resolve_confined_evidence_path`
    already bounds. An operator misconfiguring it to point at a character
    device (e.g. `/dev/zero`, which never reaches EOF) would make
    `read_bytes()` block indefinitely, hanging or OOM-killing server boot
    instead of degrading to `phase4c_unresolvable` like every other
    unreadable-path case here. A regular file over the same
    `_MAX_EVIDENCE_INPUT_BYTES` cap the phase0-3 inputs already enforce
    exercises the same guard without needing a real device file in CI."""
    import json

    from server import config as _config_module

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    phase4c_path = tmp_path / "phase4c.json"
    phase4c_path.write_bytes(b"x" * (_config_module._MAX_EVIDENCE_INPUT_BYTES + 1))
    config = _bound_config(manifest_path, phase4c_artifact_path=str(phase4c_path))
    manifest = _final_manifest(tmp_path=tmp_path, config=config, phase4c_artifact_sha256="f" * 64)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "phase4c_unresolvable"


def test_load_promotion_manifest_rejects_a_final_manifest_omitting_a_configured_phase4c_binding(
    tmp_path: Path,
) -> None:
    """Regression: Phase 4C binding only ran `if phase4c_hash is not None`,
    so an otherwise-valid `final` manifest could simply OMIT
    `phase4c_artifact_sha256` to skip byte-verification entirely, even when
    `config.phase4c_artifact_path` is set (the deployment declared it wants
    Phase 4C attestation). Omitting the field must fail closed, not be
    treated as a no-Phase-4C-needed release."""
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    phase4c_path = tmp_path / "phase4c.json"
    phase4c_path.write_text("{}", encoding="utf-8")
    config = _bound_config(manifest_path, phase4c_artifact_path=str(phase4c_path))
    manifest_without_phase4c = _final_manifest(tmp_path=tmp_path, config=config)
    assert "phase4c_artifact_sha256" not in manifest_without_phase4c
    manifest_path.write_text(json.dumps(manifest_without_phase4c), encoding="utf-8")

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "phase4c_binding_missing"


def test_load_promotion_manifest_rejects_a_final_manifest_with_a_tampered_phase0_input_hash(
    tmp_path: Path,
) -> None:
    """Regression: unlike Phase 4C (byte-verified via `sha256(path.read_bytes())`),
    the Phase 0/1/2/3 `inputs` entries were only shape-checked (a non-empty
    path string, a well-formed hex digest) -- nothing recomputed the digest
    against the actual file on disk, so a `final` manifest's declared
    evidence hashes were never proven to describe the files they name."""
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    manifest = _final_manifest(tmp_path=tmp_path, config=config)
    # Mutate the on-disk phase0 file after its hash was recorded, so the
    # manifest's declared digest no longer matches the actual bytes.
    phase0_path = tmp_path / manifest["inputs"]["phase0"]["path"]
    phase0_path.write_bytes(b"tampered-after-hashing\n")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "evidence_mismatch"


def test_load_promotion_manifest_rejects_a_final_manifest_whose_phase1_input_file_is_missing(
    tmp_path: Path,
) -> None:
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    manifest = _final_manifest(tmp_path=tmp_path, config=config)
    (tmp_path / manifest["inputs"]["phase1"]["path"]).unlink()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "evidence_unresolvable"


def test_release_version_default_matches_the_packaged_project_version() -> None:
    import tomllib

    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    with pyproject.open("rb") as handle:
        declared = tomllib.load(handle)["project"]["version"]

    assert Config().release_version == declared


def test_feature_policy_from_config_compares_by_value_across_distinct_configs() -> None:
    """`FeaturePolicy` is a frozen dataclass with structural equality; two
    field-equal `Config` objects must resolve to equal policies."""
    first = FeaturePolicy.from_config(Config(enable_autoplay_policy=False))
    second = FeaturePolicy.from_config(Config(enable_autoplay_policy=False))

    assert first == second
    assert first != FeaturePolicy.from_config(Config())


# --- Regression: manifest input bindings and explicitly-empty settings ------


def _write_manifest_for(tmp_path: Path, **overrides: object):
    import json

    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    manifest_path.write_text(
        json.dumps(_final_manifest(tmp_path=tmp_path, config=config, **overrides)), encoding="utf-8"
    )
    return config


@pytest.mark.parametrize(
    "forged_inputs",
    [
        {"phase0": "not-a-mapping"},
        {"phase0": {"sha256": "0" * 64}},
        {"phase0": {"path": "", "sha256": "0" * 64}},
        {"phase0": {"path": "phase0.jsonl", "sha256": "nope"}},
        {"phase0": {"path": "phase0.jsonl"}},
    ],
)
def test_load_promotion_manifest_rejects_unverifiable_input_entries(
    tmp_path: Path, forged_inputs: dict
) -> None:
    """Regression: `inputs` was checked only for being *a mapping*, so a
    forged-but-well-shaped manifest could declare arbitrary artifact
    bindings and still be treated as eligible."""
    load_promotion_manifest = _load_promotion_manifest()
    config = _write_manifest_for(tmp_path, inputs=forged_inputs)

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "manifest_schema_invalid"


def test_load_promotion_manifest_rejects_a_forged_schema_hash(tmp_path: Path) -> None:
    """Regression: the schema hash was only shape-checked, never recomputed."""
    load_promotion_manifest = _load_promotion_manifest()
    config = _write_manifest_for(tmp_path, schema_hash="a" * 64)

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "manifest_schema_hash_mismatch"


def test_load_promotion_manifest_rejects_a_phase3_hash_that_contradicts_its_input_entry(
    tmp_path: Path,
) -> None:
    """Regression: `phase3_completion_hash` was never compared against
    `inputs.phase3.sha256`, so the two could name different artifacts."""
    load_promotion_manifest = _load_promotion_manifest()
    config = _write_manifest_for(
        tmp_path,
        inputs={
            "phase0": {"path": "phase0.jsonl", "sha256": "0" * 64},
            "phase3": {"path": "phase3.json", "sha256": "9" * 64},
        },
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "phase3_binding_mismatch"


def test_load_promotion_manifest_rejects_a_final_manifest_with_no_phase3_input_entry(
    tmp_path: Path,
) -> None:
    load_promotion_manifest = _load_promotion_manifest()
    config = _write_manifest_for(
        tmp_path, inputs={"phase0": {"path": "phase0.jsonl", "sha256": "0" * 64}}
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "incomplete_final_manifest"


def test_load_promotion_manifest_treats_an_unreadable_schema_file_as_unverifiable_not_matching(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: an unreadable schema file returned "matches", making
    "cannot verify" indistinguishable from "verified" to the caller."""
    from server import config as config_module

    load_promotion_manifest = _load_promotion_manifest()
    config = _write_manifest_for(tmp_path)
    monkeypatch.setattr(
        config_module, "_EVIDENCE_SCHEMA_PATH", tmp_path / "no-such-schema.json", raising=True
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "manifest_schema_unverifiable"


def test_load_promotion_manifest_rejects_an_empty_string_phase4c_hash(tmp_path: Path) -> None:
    """Regression: `if phase4c_hash:` treated `""` as absent, skipping every
    Phase 4C path and hash check."""
    load_promotion_manifest = _load_promotion_manifest()
    config = _write_manifest_for(tmp_path, phase4c_artifact_sha256="")

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "phase4c_binding_mismatch"


def test_load_promotion_manifest_rejects_a_final_manifest_missing_a_phase0_binding(
    tmp_path: Path,
) -> None:
    """A `final` manifest attests the whole evidence chain, so a valid Phase 3
    binding alone is not enough."""
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path)
    # Drop phase0 from an otherwise-real, internally-consistent inputs set
    # (rather than hand-crafting placeholder phase1/2/3 entries) so this
    # exercises only the missing-phase0 completeness gate, not an incidental
    # phase3_completion_hash/inputs.phase3.sha256 mismatch.
    manifest = _final_manifest(tmp_path=tmp_path, config=config)
    del manifest["inputs"]["phase0"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "incomplete_final_manifest"


def test_load_promotion_manifest_rejects_a_malformed_generated_at_utc_with_no_deployed_at_configured(
    tmp_path: Path,
) -> None:
    """Regression: `generated_at_utc` was only parsed inside the staleness
    comparison, so with `deployed_at_utc` unset its format was never checked."""
    import json

    load_promotion_manifest = _load_promotion_manifest()
    manifest_path = tmp_path / "manifest.json"
    config = _bound_config(manifest_path, deployed_at_utc=None)
    assert config.deployed_at_utc is None
    manifest_path.write_text(
        json.dumps(
            _final_manifest(tmp_path=tmp_path, config=config, generated_at_utc="not-a-timestamp")
        ),
        encoding="utf-8",
    )

    verdict = load_promotion_manifest(config)

    assert verdict.promotion_eligible is False
    assert verdict.reason == "manifest_schema_invalid"


@pytest.mark.parametrize(
    "env_name",
    [
        "WEBSEARCH_EARLY_ACK_TEXT",
        "WEBSEARCH_PROMOTION_MANIFEST_PATH",
        "WEBSEARCH_RELEASE_VERSION",
    ],
)
def test_load_config_explicitly_empty_string_settings_reach_validation(env_name: str) -> None:
    """Regression: these settings were read with walrus-truthiness, so an
    operator setting them to "" silently got the default instead of reaching
    `Config.__post_init__`'s empty-value validation."""
    with pytest.raises(ConfigError):
        load_config(env={env_name: ""})


def test_load_config_explicitly_empty_identity_settings_are_not_silently_defaulted() -> None:
    """The identity fields have no `__post_init__` rejection, but an explicit
    empty value must still be carried through as unset-and-unbound rather
    than dropped as if the operator had said nothing."""
    config = load_config(env={"PIPECAT_SOURCE_COMMIT": "", "PIPECAT_SOURCE_TREE_HASH": ""})

    assert config.source_commit == ""
    assert config.source_tree_hash == ""

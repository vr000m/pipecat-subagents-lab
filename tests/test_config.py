"""Contract tests for operator configuration.

These tests deliberately exercise the configuration boundary rather than
environment-specific defaults.  They are test-first: the Phase 1
implementation is expected to provide ``Config`` and ``load_config``.
"""

import re
from pathlib import Path

import pytest

from server.config import (
    Config,
    ConfigError,
    FeaturePolicy,
    default_reasoning_effort_for_model,
    load_config,
)


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


def test_operator_reasoning_effort_loads_from_toml_and_environment_wins(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        '[models]\nrouter_reasoning_effort = "low"\nworker_reasoning_effort = "high"\n'
    )

    config = load_config(
        config_file=config_file,
        env={"WEBSEARCH_ROUTER_REASONING_EFFORT": "xhigh"},
    )

    assert config.resolve_router_reasoning_effort("fast") == "xhigh"
    assert config.resolve_worker_reasoning_effort("deep") == "high"


def test_toml_reasoning_effort_applies_to_a_toml_configured_model_label(tmp_path) -> None:
    # The reasoning-effort TOML key must resolve against whatever label the
    # same load_config() call registered for router_model/worker_model
    # (via _registered_policy_labels), not a hardcoded "fast"/"deep" guess --
    # same invariant WEBSEARCH_ROUTER_REASONING_EFFORT's env path already
    # covers (test_websearch_router_reasoning_effort_env_applies_to_the_
    # registered_label), mirrored here for the TOML source.
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        "[models]\n"
        'router_model = "toml-router"\n'
        'router_reasoning_effort = "low"\n'
        'worker_model = "toml-worker"\n'
        'worker_reasoning_effort = "medium"\n'
    )

    config = load_config(config_file=config_file, env={})

    assert config.resolve_router_reasoning_effort("fast") == "low"
    assert config.resolve_worker_reasoning_effort("deep") == "medium"


def test_reasoning_effort_resolver_unknown_model_policy_label_raises_config_error() -> None:
    config = Config(
        router_model_policy={"fast": "verified-router-model"},
        worker_model_policy={"deep": "verified-worker-model"},
    )

    with pytest.raises(ConfigError):
        config.resolve_router_reasoning_effort("model-emitted-id")
    with pytest.raises(ConfigError):
        config.resolve_worker_reasoning_effort("model-emitted-id")


def test_reasoning_effort_resolver_registered_label_without_effort_entry_resolves_to_none() -> None:
    config = Config(
        router_model_policy={"fast": "verified-router-model"},
        worker_model_policy={
            "deep": "verified-worker-model",
            "custom-worker-label": "verified-custom-worker-model",
        },
    )

    assert config.resolve_router_reasoning_effort("fast") is None
    assert config.resolve_worker_reasoning_effort("deep") is None
    assert config.resolve_worker_reasoning_effort("custom-worker-label") is None


def test_reasoning_effort_resolver_explicit_override_is_honored_end_to_end() -> None:
    config = Config(
        router_model_policy={"fast": "non-gpt-5-router-model"},
        worker_model_policy={"deep": "verified-worker-model"},
        router_reasoning_effort_policy={"fast": "high"},
        worker_reasoning_effort_policy={"deep": "low"},
    )

    assert config.resolve_router_reasoning_effort("fast") == "high"
    assert config.resolve_worker_reasoning_effort("deep") == "low"


def test_reasoning_effort_policy_rejects_value_outside_sdk_documented_literal() -> None:
    with pytest.raises(ConfigError):
        Config(router_reasoning_effort_policy={"fast": "extreme"})
    with pytest.raises(ConfigError):
        Config(worker_reasoning_effort_policy={"deep": "extreme"})


def test_reasoning_effort_policy_rejects_a_label_not_in_model_policy() -> None:
    """A typo'd effort-policy label (e.g. "fastt" instead of "fast") would
    otherwise be silently accepted and never read by
    resolve_router_reasoning_effort -- the two dicts must agree on labels."""
    with pytest.raises(ConfigError):
        Config(
            router_model_policy={"fast": "gpt-5-mini"},
            router_reasoning_effort_policy={"fastt": "high"},
        )
    with pytest.raises(ConfigError):
        Config(
            worker_model_policy={"deep": "gpt-5"},
            worker_reasoning_effort_policy={"deeep": "low"},
        )


def test_reasoning_effort_policy_allows_a_subset_of_model_policy_labels() -> None:
    """A model-policy label with no matching effort-policy entry is fine (the
    documented "resolves to None" case) -- only an effort-policy label absent
    from the model policy is rejected."""
    config = Config(
        router_model_policy={"fast": "gpt-5-mini", "extra-label": "some-model"},
        router_reasoning_effort_policy={"fast": "high"},
    )
    assert config.resolve_router_reasoning_effort("fast") == "high"
    assert config.resolve_router_reasoning_effort("extra-label") is None


@pytest.mark.parametrize("effort", ["none", "minimal", "low", "medium", "high", "xhigh", "max"])
def test_reasoning_effort_policy_accepts_the_full_sdk_documented_literal(effort: str) -> None:
    """The OpenAI SDK's ``ReasoningEffort`` literal is broader than the four
    values named in the plan's Objective; validation must accept all of it,
    not a hardcoded subset."""
    config = Config(router_reasoning_effort_policy={"fast": effort})

    assert config.resolve_router_reasoning_effort("fast") == effort


def test_websearch_worker_reasoning_effort_env_applies_to_a_non_deep_registered_label() -> None:
    """Regression for round 8 gauntlet, Logic lens finding 4:
    WEBSEARCH_WORKER_REASONING_EFFORT previously replaced
    worker_reasoning_effort_policy wholesale with {"deep": raw}, tripping
    the label cross-validation ConfigError for any supported worker model
    policy whose label isn't literally "deep". The override must apply to
    whatever label(s) are actually registered.
    """
    config = load_config(
        env={
            "OPENAI_API_KEY": "test-key",
            "WEBSEARCH_WORKER_MODEL": "custom-worker-model",
            "WEBSEARCH_WORKER_REASONING_EFFORT": "high",
        }
    )
    # WEBSEARCH_WORKER_MODEL registers {"deep": "custom-worker-model"} (its
    # own env override also hardcodes the "deep" label), so this case still
    # exercises "deep" -- the point is the two overrides now agree with each
    # other via the shared registered-label lookup rather than by
    # coincidence.
    assert config.worker_model_policy == {"deep": "custom-worker-model"}
    assert config.worker_reasoning_effort_policy == {"deep": "high"}
    assert config.resolve_worker_reasoning_effort("deep") == "high"


def test_websearch_router_reasoning_effort_env_applies_to_the_registered_label() -> None:
    config = load_config(
        env={
            "OPENAI_API_KEY": "test-key",
            "WEBSEARCH_ROUTER_REASONING_EFFORT": "low",
        }
    )
    assert config.router_reasoning_effort_policy == {"fast": "low"}


def test_registered_policy_labels_falls_back_to_the_field_default_when_kwargs_omit_it() -> None:
    """Direct unit coverage of the lookup helper itself: with no
    WEBSEARCH_WORKER_MODEL override present in kwargs, the effort override
    must still target the dataclass field's own default label ("deep"), not
    an empty/hardcoded guess.
    """
    from server.config import _registered_policy_labels

    assert _registered_policy_labels({}, "worker_model_policy") == ("deep",)
    assert _registered_policy_labels({}, "router_model_policy") == ("fast",)
    assert _registered_policy_labels(
        {"worker_model_policy": {"custom": "some-model"}}, "worker_model_policy"
    ) == ("custom",)


def test_config_constructed_positionally_with_pre_existing_field_order_still_works() -> None:
    """Regression: the reasoning-effort policy fields must be appended after
    every pre-existing field (including the diagnostic fields at the very
    end), not inserted mid-dataclass -- otherwise this positional
    construction, which mirrors the field order Phase 1 found in place,
    would silently shift arguments instead of raising or behaving
    identically to the default construction below."""
    import dataclasses

    import server.config as _config_module

    config = Config(
        None,  # openai_api_key
        "OPENAI_API_KEY",  # openai_api_key_env
        None,  # deepgram_api_key
        None,  # cartesia_api_key
        None,  # cartesia_voice_id
        {"fast": "gpt-5-mini"},  # router_model_policy
        {"deep": "gpt-5"},  # worker_model_policy
        2,  # max_work_items_per_turn
        10_000,  # multi_intent_wait_timeout_ms
        15.0,  # foreground_search_timeout_seconds
        12.0,  # router_timeout_seconds
        75.0,  # provider_timeout_seconds
        2.0,  # shutdown_grace_seconds
        12,  # max_citations
        30.0,  # pending_dialogue_timeout_seconds
        "websocket",  # stt_service
        "local",  # stt_provider
        "nova-3-general",  # stt_model
        "en",  # stt_language
        None,  # stt_endpoint
        5.0,  # smart_turn_timeout_seconds
        1.5,  # smart_turn_complete_grace_seconds
        10.0,  # speech_start_timeout_seconds
        1.0,  # speech_transport_grace_seconds
        None,  # tts_endpoint
        "local",  # tts_provider
        "sonic-3.5",  # tts_model
        "azelma",  # tts_voice_id
        "127.0.0.1",  # bind_host
        7860,  # bind_port
        "http://127.0.0.1:7860",  # known_client_url
        True,  # enable_early_ack
        True,  # enable_background_status
        True,  # enable_autoplay_policy
        "One moment while I look into that.",  # early_ack_text
        _config_module._DEFAULT_RELEASE_VERSION,  # release_version
        None,  # source_commit
        None,  # source_tree_hash
        None,  # deployed_at_utc
    )

    assert config == Config()

    field_names = [f.name for f in dataclasses.fields(Config)]
    deployed_at_utc_index = field_names.index("deployed_at_utc")
    for new_field in ("router_reasoning_effort_policy", "worker_reasoning_effort_policy"):
        assert field_names.index(new_field) > deployed_at_utc_index


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
    assert config.resolve_router_model("fast") == "gpt-5.6-luna"
    assert config.resolve_worker_model("deep") == "gpt-5.6-terra"
    assert config.resolve_router_reasoning_effort("fast") == "medium"
    assert config.resolve_worker_reasoning_effort("deep") == "medium"


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


def test_release_version_default_matches_the_packaged_project_version() -> None:
    import tomllib

    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    with pyproject.open("rb") as handle:
        declared = tomllib.load(handle)["project"]["version"]

    assert Config().release_version == declared


def test_missing_package_metadata_falls_back_to_pyproject_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import server.config as config_module

    def package_version_missing(_name: str) -> str:
        raise config_module.PackageNotFoundError("test package metadata is absent")

    monkeypatch.setattr(config_module, "_package_version", package_version_missing)

    assert config_module._installed_release_version() == "1.0.1"


def test_feature_policy_from_config_compares_by_value_across_distinct_configs() -> None:
    """`FeaturePolicy` is a frozen dataclass with structural equality; two
    field-equal `Config` objects must resolve to equal policies."""
    first = FeaturePolicy.from_config(Config(enable_autoplay_policy=False))
    second = FeaturePolicy.from_config(Config(enable_autoplay_policy=False))

    assert first == second
    assert first != FeaturePolicy.from_config(Config())


@pytest.mark.parametrize(
    "env_name",
    [
        "WEBSEARCH_EARLY_ACK_TEXT",
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


def test_toml_features_release_version_reaches_config(tmp_path: Path) -> None:
    """Regression: ``[features].release_version`` is documented in README
    alongside ``WEBSEARCH_RELEASE_VERSION``, but ``_load_toml_values`` never
    copied it, so an operator's override was silently dropped and
    ``load_config`` fell back to the packaged default."""
    config_file = tmp_path / "config.toml"
    config_file.write_text('[features]\nrelease_version = "0.1.3-rc4"\n')

    config = load_config(config_file=config_file, env={})

    assert config.release_version == "0.1.3-rc4"


def test_environment_release_version_overrides_the_toml_value(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text('[features]\nrelease_version = "0.1.3-from-toml"\n')

    config = load_config(
        config_file=config_file, env={"WEBSEARCH_RELEASE_VERSION": "0.1.3-from-env"}
    )

    assert config.release_version == "0.1.3-from-env"


# --- Round 10 gauntlet finding 2: auto-clear inherited effort on a --------
# model-only override at a higher-precedence layer.


class TestReasoningEffortInheritance:
    def test_env_model_override_clears_toml_effort(self, tmp_path: Path) -> None:
        # Deliberately a DIFFERING model ("gpt-5.9-other" vs TOML's "gpt-5"):
        # round 10 gauntlet, Logic finding 3 made provenance advance only
        # when a layer's model value actually changes the effective model,
        # so a same-value re-assertion here would no longer clear the
        # inherited effort (see
        # test_same_value_model_reassertion_does_not_clear_the_shipped_effort
        # below for that case).
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nworker_model = "gpt-5"\nworker_reasoning_effort = "medium"\n'
            'router_model = "gpt-5-mini"\nrouter_reasoning_effort = "minimal"\n'
        )

        config = load_config(
            config_file=config_file, env={"WEBSEARCH_WORKER_MODEL": "gpt-5.9-other"}
        )

        assert config.worker_reasoning_effort_policy == {}
        assert config.resolve_worker_reasoning_effort("deep") is None
        # The router pair is untouched by the worker-only override.
        assert config.router_reasoning_effort_policy == {"fast": "minimal"}

    def test_same_layer_model_and_effort_both_apply(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text('[models]\nworker_model = "gpt-5"\n')

        config = load_config(
            config_file=config_file,
            env={
                "WEBSEARCH_WORKER_MODEL": "gpt-5.1",
                "WEBSEARCH_WORKER_REASONING_EFFORT": "high",
            },
        )

        assert config.resolve_worker_reasoning_effort("deep") == "high"

    def test_effort_only_override_applies_over_toml_model(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text('[models]\nworker_model = "gpt-5"\n')

        config = load_config(
            config_file=config_file, env={"WEBSEARCH_WORKER_REASONING_EFFORT": "low"}
        )

        assert config.resolve_worker_reasoning_effort("deep") == "low"

    def test_env_file_model_override_clears_toml_effort(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nworker_model = "gpt-5"\nworker_reasoning_effort = "medium"\n'
        )

        config = load_config(
            config_file=config_file,
            env_file={"WEBSEARCH_WORKER_MODEL": "gpt-5.1"},
            env={},
        )

        assert config.resolve_worker_reasoning_effort("deep") is None

    def test_explicitly_empty_model_override_does_not_clear_toml_effort(
        self, tmp_path: Path
    ) -> None:
        config_file = tmp_path / "config.toml"
        # Deliberately NOT "gpt-5" (the worker_model_policy dataclass default):
        # an identical value made the model half of this pairing untestable.
        config_file.write_text(
            '[models]\nworker_model = "gpt-5.2-orion"\nworker_reasoning_effort = "medium"\n'
        )

        config = load_config(config_file=config_file, env={"WEBSEARCH_WORKER_MODEL": ""})

        assert config.resolve_worker_reasoning_effort("deep") == "medium"
        # Round 11 gauntlet, Minor B: the surviving effort must stay paired with
        # the TOML model it was configured against, not the dataclass default.
        assert config.resolve_worker_model("deep") == "gpt-5.2-orion"

    def test_whitespace_only_model_override_does_not_erase_the_toml_layer(
        self, tmp_path: Path
    ) -> None:
        # Round 3 confirming pass, Logic finding 3: _effectively_set() is
        # deliberately stricter than the truthiness-walrus consumers (a
        # whitespace-only string is truthy in Python), so a whitespace-only
        # override must be treated as absent here too, not just an empty one.
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nworker_model = "gpt-5.2-orion"\nworker_reasoning_effort = "medium"\n'
        )

        config = load_config(config_file=config_file, env={"WEBSEARCH_WORKER_MODEL": "   "})

        assert config.resolve_worker_reasoning_effort("deep") == "medium"
        assert config.resolve_worker_model("deep") == "gpt-5.2-orion"

    def test_explicitly_empty_effort_override_does_not_clear_the_toml_effort(
        self, tmp_path: Path
    ) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nworker_model = "gpt-5.2-orion"\nworker_reasoning_effort = "medium"\n'
        )

        config = load_config(config_file=config_file, env={"WEBSEARCH_WORKER_REASONING_EFFORT": ""})

        assert config.resolve_worker_model("deep") == "gpt-5.2-orion"
        assert config.resolve_worker_reasoning_effort("deep") == "medium"

    def test_no_overrides_toml_pair_applies_unchanged(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nworker_model = "gpt-5"\nworker_reasoning_effort = "medium"\n'
        )

        config = load_config(config_file=config_file, env={})

        assert config.resolve_worker_reasoning_effort("deep") == "medium"

    def test_a_falsy_typed_toml_model_does_not_clear_an_inherited_effort(
        self, tmp_path: Path
    ) -> None:
        """Round 5 restart2, Logic L6: _effectively_set() used to be
        `value is not None and str(value).strip() != ""`, so a typed falsy
        value (a TOML boolean/int -- `False`/`0`/`[]`, `str()`-ified to a
        non-empty "False"/"0"/"[]") counted as SET, even though every
        consumer's truthiness walrus reads it as absent. That let a falsy
        typed value record real provenance in _record_layer, which
        _clear_inherited_reasoning_effort then read to delete a real
        lower-layer effort key on the strength of a "model override" no
        consumer would ever read.

        Env values are always strings in real usage, so the only place a
        genuinely typed (non-str) value can reach a HIGHER-precedence layer
        than config.toml's own [models] table is a caller that constructs
        the ``env``/``env_file`` mapping programmatically -- exotic, but
        real: ``load_config`` accepts ``Mapping[str, str]`` and nothing
        enforces the value type at runtime. This reproduces exactly that:
        a higher layer overriding only the model with a typed `False` must
        be treated exactly like an absent override, so the lower TOML
        layer's model AND its inherited effort both survive.
        """
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nworker_model = "gpt-5.2-orion"\nworker_reasoning_effort = "medium"\n'
        )

        config = load_config(config_file=config_file, env={"WEBSEARCH_WORKER_MODEL": False})  # type: ignore[dict-item]

        assert config.resolve_worker_model("deep") == "gpt-5.2-orion"
        assert config.resolve_worker_reasoning_effort("deep") == "medium"

    def test_same_value_model_reassertion_does_not_clear_the_shipped_effort(
        self, tmp_path: Path
    ) -> None:
        """Round 10 gauntlet, Logic finding 3: re-asserting a role's model at
        a higher-precedence layer to its ALREADY-SHIPPED value must not
        advance that key's provenance, so the shipped effort survives.
        Concrete trigger: the justfile ``run`` recipe sourcing an env file
        that pins ``WEBSEARCH_WORKER_MODEL`` to exactly config.toml's
        default.
        """
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nworker_model = "gpt-5.6-terra"\nworker_reasoning_effort = "medium"\n'
        )

        config = load_config(
            config_file=config_file, env={"WEBSEARCH_WORKER_MODEL": "gpt-5.6-terra"}
        )

        assert config.resolve_worker_reasoning_effort("deep") == "medium"
        assert config.resolve_worker_model("deep") == "gpt-5.6-terra"

    def test_differing_value_model_reassertion_still_clears_the_shipped_effort(
        self, tmp_path: Path
    ) -> None:
        """Pins that the existing revert-to-baseline recipe is untouched by
        the same-value fix above -- a genuinely differing model override
        must still clear the inherited effort.
        """
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nworker_model = "gpt-5.6-terra"\nworker_reasoning_effort = "medium"\n'
        )

        config = load_config(config_file=config_file, env={"WEBSEARCH_WORKER_MODEL": "gpt-5"})

        assert config.resolve_worker_reasoning_effort("deep") is None

    def test_same_value_model_reassertion_with_an_explicit_effort_lets_the_effort_win(
        self, tmp_path: Path
    ) -> None:
        """Pins the deliberate asymmetry: an effort key's provenance still
        advances on every effectively-set value, even when its paired model
        key's provenance does not (because the model didn't change). An env
        layer that re-asserts the same model AND explicitly sets an effort
        must have that effort win, not be silently cleared by
        _clear_inherited_reasoning_effort comparing stale layer numbers.
        """
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nworker_model = "gpt-5.6-terra"\nworker_reasoning_effort = "medium"\n'
        )

        config = load_config(
            config_file=config_file,
            env={
                "WEBSEARCH_WORKER_MODEL": "gpt-5.6-terra",
                "WEBSEARCH_WORKER_REASONING_EFFORT": "high",
            },
        )

        assert config.resolve_worker_reasoning_effort("deep") == "high"

    def test_same_value_router_model_reassertion_does_not_clear_the_shipped_effort(
        self, tmp_path: Path
    ) -> None:
        """Router-role mirror of the worker case above -- both roles go
        through the same _record_layer/_apply_layer loop.
        """
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nrouter_model = "gpt-5-mini"\nrouter_reasoning_effort = "minimal"\n'
        )

        config = load_config(config_file=config_file, env={"WEBSEARCH_ROUTER_MODEL": "gpt-5-mini"})

        assert config.router_reasoning_effort_policy == {"fast": "minimal"}

    def test_provenance_keys_alone_drive_layer_recording(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Round 5 restart2, Architecture A6: object-identity alone (round-4
        restart's fix) buys nothing -- both constants are still derived from
        the same source tuple, so adding a role changes both simultaneously.
        What actually matters is that _PROVENANCE_KEYS is what _record_layer
        reads, proven here by zeroing it out: with no keys recorded, a higher
        layer that overrides only WEBSEARCH_WORKER_MODEL can no longer be
        told apart from "the model came along with the effort", so
        _clear_inherited_reasoning_effort must NOT clear the inherited TOML
        effort.
        """
        import server.config as _config_module

        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nworker_model = "gpt-5.2-orion"\nworker_reasoning_effort = "medium"\n'
        )

        # Sanity: with the real _PROVENANCE_KEYS, a model-only override at a
        # higher layer DOES clear the inherited effort.
        baseline_config = load_config(
            config_file=config_file, env={"WEBSEARCH_WORKER_MODEL": "gpt-5"}
        )
        assert baseline_config.resolve_worker_reasoning_effort("deep") is None

        monkeypatch.setattr(_config_module, "_PROVENANCE_KEYS", ())

        config = load_config(config_file=config_file, env={"WEBSEARCH_WORKER_MODEL": "gpt-5"})

        assert config.resolve_worker_reasoning_effort("deep") == "medium"

    def test_empty_means_absent_keys_alone_drive_the_empty_override_skip(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Round 5 restart2, Architecture A6: proves _EMPTY_MEANS_ABSENT_KEYS
        is what _apply_layer reads, by zeroing it out and reproducing the
        round-11 gauntlet Minor B regression it was added to fix -- an
        effectively-empty WEBSEARCH_WORKER_MODEL="" override now lands in
        `values` as a real write again, erasing the TOML model down to the
        dataclass default.
        """
        import server.config as _config_module

        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[models]\nworker_model = "gpt-5.2-orion"\nworker_reasoning_effort = "medium"\n'
        )

        # Sanity: with the real _EMPTY_MEANS_ABSENT_KEYS, the empty override
        # is a no-op and the TOML model survives.
        baseline_config = load_config(config_file=config_file, env={"WEBSEARCH_WORKER_MODEL": ""})
        assert baseline_config.resolve_worker_model("deep") == "gpt-5.2-orion"

        monkeypatch.setattr(_config_module, "_EMPTY_MEANS_ABSENT_KEYS", ())

        config = load_config(config_file=config_file, env={"WEBSEARCH_WORKER_MODEL": ""})

        assert config.resolve_worker_model("deep") == "gpt-5"

    def test_layer_zero_toml_also_honours_the_empty_override_gate(self, tmp_path: Path) -> None:
        """Round 6 gauntlet, Logic G2 / Architecture A1: before this fix, the
        TOML layer (layer 0) bypassed `_apply_layer` entirely -- it wrote
        straight into `values` via `_load_toml_values`, then called
        `_record_layer` separately, so the empty-override gate that layers 1
        and 2 get for free never applied to layer 0.

        That was a behavioural no-op *for layer 0 specifically* (nothing
        beneath it to erase), but it made a shape representable that
        `_apply_layer` makes unrepresentable for every other layer: a
        whitespace-only TOML value landing in `values` while `_record_layer`
        (which already applies `_effectively_set`) declines to record its
        provenance. Now that layer 0 routes through `_apply_layer` like the
        other two, a whitespace-only `router_model` must never land in
        `values` at all, and the real `router_reasoning_effort` set alongside
        it must survive untouched.
        """
        config_file = tmp_path / "config.toml"
        config_file.write_text('[models]\nrouter_model = "   "\nrouter_reasoning_effort = "high"\n')

        config = load_config(config_file=config_file, env={})

        assert config.resolve_router_model("fast") != "   "
        assert config.resolve_router_reasoning_effort("fast") == "high"

    def test_empty_means_absent_keys_are_a_subset_of_provenance_keys(self) -> None:
        """Every key that treats an empty override as "absent" must also carry
        provenance -- otherwise _clear_inherited_reasoning_effort would reason
        about a layer number that was never recorded (round-5 restart, Logic
        Important).

        The family keys are deliberately in NEITHER constant: they resolve
        against the raw per-layer mappings rather than through _record_layer's
        provenance dict, so a hand-written roster of them can no longer drift
        from the member tuples load_config actually resolves (round 6 confirm
        pass 3, Architecture Minor).

        The roster iterates the `_families()` registry rather than naming the
        two endpoint constants by hand: the hand-written version left the
        alias families unpinned and would have left a third endpoint family
        added later outside the assertion too (round 7 confirm pass 4,
        Architecture Minor)."""
        import server.config as _config_module

        assert set(_config_module._EMPTY_MEANS_ABSENT_KEYS) <= set(_config_module._PROVENANCE_KEYS)
        families = _config_module._families()
        family_keys = {
            key for _name, members in families for key in _config_module._family_keys(members)
        }
        # Pins that the registry actually covers all three kinds of family --
        # otherwise an empty/partial registry would satisfy the disjointness
        # assertions below vacuously.
        assert {"WEBSEARCH_STT_ENDPOINT", "WEBSEARCH_TTS_WS_PORT"} <= family_keys
        assert {"WEBSEARCH_DEEPGRAM_API_KEY", "DEEPGRAM_API_KEY"} <= family_keys
        assert {"WEBSEARCH_OPENAI_API_KEY", "OPENAI_API_KEY"} <= family_keys
        assert not family_keys & set(_config_module._PROVENANCE_KEYS)
        assert not family_keys & set(_config_module._EMPTY_MEANS_ABSENT_KEYS)

    def test_alias_family_registry_covers_every_alias_config_field(self) -> None:
        """`_alias_families` is the single roster load_config iterates, so a
        credential alias added to `_ALIAS_FAMILY_SPELLINGS` reaches resolution
        without a second edit -- and every field it names must be a real
        Config field (round 7 confirm pass 4, Architecture Minor)."""
        import dataclasses

        import server.config as _config_module

        field_names = {f.name for f in dataclasses.fields(_config_module.Config)}
        alias_fields = {field for field, _members in _config_module._alias_families()}
        assert alias_fields <= field_names
        assert "openai_api_key" in alias_fields

    def test_runtime_named_openai_key_joins_the_same_registry(self) -> None:
        """The runtime-named bare spelling replaces the default one in the
        registry rather than being assembled as a one-off pair."""
        import server.config as _config_module

        families = dict(_config_module._alias_families("MY_OPENAI_KEY"))
        assert _config_module._family_keys(
            _config_module._widen_members(families["openai_api_key"])
        ) == (
            "WEBSEARCH_OPENAI_API_KEY",
            "MY_OPENAI_KEY",
        )
        # Only the row that declares a `None` bare key takes the runtime name;
        # the others keep their declared vendor spelling (round 8 confirm
        # pass 5, Architecture Minor: the property moved into the data).
        assert _config_module._family_keys(
            _config_module._widen_members(families["deepgram_api_key"])
        ) == (
            "WEBSEARCH_DEEPGRAM_API_KEY",
            "DEEPGRAM_API_KEY",
        )

    def test_every_endpoint_member_constant_is_registered(self) -> None:
        """A `_*_ENDPOINT_MEMBERS` constant that never reached
        `_ENDPOINT_FAMILIES` would be resolved by nothing and enumerated by
        nothing -- `load_config` iterates that registry, so an unregistered
        family is simply dead (round 8 confirm pass 5, Architecture Minor).

        A naming-convention scan can only see constants that FOLLOW the
        convention, so this is deliberately the weaker half of a pair:
        `test_load_config_consumes_every_registered_endpoint_family` below
        catches a registered family that never reaches `kwargs` regardless of
        what its member constant is called, or whether it is a module constant
        at all (round 9 confirm pass 6, Architecture Minor).

        Comparison is by VALUE, not `id()`: a constant registered as an
        equal-but-distinct tuple literal is registered, and reporting it as
        missing would be a confusing failure mode for a backstop test."""
        import server.config as _config_module

        declared = {
            name: value
            for name, value in vars(_config_module).items()
            if name.startswith("_") and name.endswith("_ENDPOINT_MEMBERS")
        }
        assert declared, "no `_*_ENDPOINT_MEMBERS` constants found -- has the naming changed?"
        registered = [members for _field, members, _build in _config_module._ENDPOINT_FAMILIES]
        unregistered = sorted(name for name, value in declared.items() if value not in registered)
        assert not unregistered, (
            f"{unregistered} declared but absent from _ENDPOINT_FAMILIES -- "
            "load_config resolves only what that registry names"
        )

    def test_load_config_consumes_every_registered_endpoint_family(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Round 9 confirm pass 6, Architecture Important: `_ENDPOINT_FAMILIES`
        consolidated RESOLUTION but not CONSUMPTION. `load_config` looped the
        registry to compute each family's winner and then read only the two
        hand-written literals `endpoint_resolution["stt_endpoint"]` /
        `["tts_endpoint"]` back out, so a third registered family resolved
        correctly and was then silently dropped -- the operator's configured
        endpoint replaced by the dataclass default, with the registration scan
        above still green.

        This pins the invariant that actually matters: every registered row's
        builder is invoked, and what it returns lands on the Config field the
        row names."""
        import server.config as _config_module

        original = _config_module._ENDPOINT_FAMILIES
        called: list[str] = []
        sentinel = ("ws", "sentinel.invalid:9")

        def _wrap(
            field: str,
            build: object,
        ) -> object:
            def _wrapped(member: str | None, values: object) -> object:
                called.append(field)
                # The first row's builder is replaced outright so the return
                # value can be traced to the field it was registered under.
                if field == original[0][0]:
                    return sentinel
                return build(member, values)  # type: ignore[operator]

            return _wrapped

        monkeypatch.setattr(
            _config_module,
            "_ENDPOINT_FAMILIES",
            tuple((field, members, _wrap(field, build)) for field, members, build in original),
        )

        config = _config_module.load_config(env={})

        assert called == [field for field, _members, _build in original], (
            "load_config must invoke every registered family's builder, in registry order -- "
            "a row it never consumes resolves and then vanishes"
        )
        assert getattr(config, original[0][0]) == sentinel, (
            f"the builder registered for {original[0][0]!r} returned a value that never "
            "reached that Config field"
        )


class TestDefaultReasoningEffortForModel:
    """Round 10 gauntlet, Architecture finding 4: the gpt-5*-naming rule,
    hoisted out of ``server.router.effective_router_reasoning_effort`` so a
    non-router caller (the eval judge) can share it without depending on the
    router's own resolved-config precedence."""

    def test_gpt5_family_defaults_to_minimal(self) -> None:
        assert default_reasoning_effort_for_model("gpt-5.6-luna") == "minimal"
        assert default_reasoning_effort_for_model("gpt-5-mini") == "minimal"

    def test_non_gpt5_model_has_no_default(self) -> None:
        assert default_reasoning_effort_for_model("gpt-4.1-mini") is None


class TestExplicitZeroNumericOverrides:
    """Round-5 restart gauntlet, Logic Important: a truthiness walrus treated an
    operator's explicit numeric ``0`` as absent, substituted the packaged
    default, and so bypassed the ``__post_init__`` validation that exists
    precisely to reject that value by name."""

    def test_toml_zero_max_citations_reaches_validation(self, tmp_path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text("[turn]\nmax_citations = 0\n")

        with pytest.raises(ConfigError, match="max_citations must be between 1 and 50"):
            load_config(config_file=config_file, env={})

    def test_toml_zero_smart_turn_timeout_reaches_validation(self, tmp_path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text("[turn]\nsmart_turn_timeout_seconds = 0.0\n")

        with pytest.raises(ConfigError, match="smart_turn_timeout_seconds must be between"):
            load_config(config_file=config_file, env={})

    def test_toml_zero_smart_turn_complete_grace_reaches_validation(self, tmp_path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text("[turn]\nsmart_turn_complete_grace_seconds = 0.0\n")

        with pytest.raises(ConfigError, match="smart_turn_complete_grace_seconds must be between"):
            load_config(config_file=config_file, env={})

    def test_non_numeric_max_work_items_raises_config_error_not_bare_value_error(self) -> None:
        """Round-5 restart, Logic Minor: the two unwrapped ``int()`` calls
        escaped ``load_config`` as a bare ValueError naming no field."""
        with pytest.raises(ConfigError, match="WEBSEARCH_MAX_WORK_ITEMS_PER_TURN must be"):
            load_config(env={"WEBSEARCH_MAX_WORK_ITEMS_PER_TURN": "many"})

    def test_non_numeric_multi_intent_wait_timeout_raises_config_error(self) -> None:
        with pytest.raises(ConfigError, match="WEBSEARCH_MULTI_INTENT_WAIT_TIMEOUT_MS must be"):
            load_config(env={"WEBSEARCH_MULTI_INTENT_WAIT_TIMEOUT_MS": "soon"})


class TestEndpointFamilyLayerPrecedence:
    """Round-5 restart gauntlet, Logic Important: the STT/TTS endpoint families
    resolved by hardcoded key priority, so a lower-precedence layer's spelling
    silently beat a higher-precedence layer's override."""

    def test_process_env_tts_socket_beats_toml_tts_uri(self, tmp_path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text('[tts]\ntts_ws_uri = "wss://tts.example.test:9443"\n')

        config = load_config(
            config_file=config_file,
            env={"WEBSEARCH_TTS_WS_SOCKET": "/tmp/override-tts.sock"},
        )

        assert config.tts_endpoint == ("uds", "/tmp/override-tts.sock")

    def test_process_env_tts_host_port_beats_toml_tts_uri(self, tmp_path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text('[tts]\ntts_ws_uri = "wss://tts.example.test:9443"\n')

        config = load_config(
            config_file=config_file,
            env={
                "WEBSEARCH_TTS_WS_HOST": "127.0.0.1",
                "WEBSEARCH_TTS_WS_PORT": "9100",
            },
        )

        assert config.tts_endpoint == ("ws", "127.0.0.1:9100")

    def test_process_env_stt_socket_beats_env_file_stt_endpoint(self) -> None:
        config = load_config(
            env_file={"WEBSEARCH_STT_ENDPOINT": "ws://stt.example.test:9001"},
            env={"WEBSEARCH_STT_WS_SOCKET": "/tmp/override-stt.sock"},
        )

        assert config.stt_endpoint == ("uds", "/tmp/override-stt.sock")

    def test_key_priority_still_breaks_a_same_layer_tie(self, tmp_path) -> None:
        """The README's documented within-layer order (URI > socket > host:port)
        is unchanged; layer precedence only outranks it across layers."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tts]\ntts_ws_uri = "wss://tts.example.test:9443"\n'
            'tts_ws_socket = "~/tts.sock"\n'
            'tts_ws_host = "ignored.example.test"\ntts_ws_port = 9000\n'
        )

        config = load_config(config_file=config_file, env={})

        assert config.tts_endpoint == ("wss", "tts.example.test:9443")

    def test_tts_host_without_port_is_loud(self) -> None:
        """Round-5 restart, Logic Minor: a half-specified pair was the only
        malformed endpoint input that failed open onto the dataclass default."""
        with pytest.raises(ConfigError, match="must be set together"):
            load_config(env={"WEBSEARCH_TTS_WS_HOST": "127.0.0.1"})

    def test_tts_port_without_host_is_loud(self) -> None:
        with pytest.raises(ConfigError, match="must be set together"):
            load_config(env={"WEBSEARCH_TTS_WS_PORT": "9100"})

    def test_stray_toml_host_no_longer_breaks_a_higher_layer_endpoint(self, tmp_path) -> None:
        """Round 6 confirm pass 3, Logic Important: the pairing check fired
        before family resolution, so a leftover half-pair at a LOWER layer --
        never consulted, before or after the round-5 fix -- turned a config
        that booted fine into a hard startup failure."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('[tts]\ntts_ws_host = "legacy.example.test"\n')

        config = load_config(
            config_file=config_file,
            env={"WEBSEARCH_TTS_ENDPOINT": "ws://tts.example.test:9002"},
        )

        assert config.tts_endpoint == ("ws", "tts.example.test:9002")

    def test_same_layer_stray_host_loses_to_key_priority_instead_of_failing(self, tmp_path) -> None:
        """A half-pair beside a higher-priority spelling in the SAME layer is
        resolved by the documented key priority, exactly as a complete pair
        would be -- it is not consulted, so it is not malformed input."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tts]\ntts_ws_uri = "wss://tts.example.test:9443"\ntts_ws_host = "orphan"\n'
        )

        config = load_config(config_file=config_file, env={})

        assert config.tts_endpoint == ("wss", "tts.example.test:9443")

    def test_half_pair_above_the_winning_member_is_still_loud(self, tmp_path) -> None:
        """The loudness the round-5 fix added survives where it matters: a
        half-pair at a HIGHER layer than the winner is the operator's
        highest-precedence intent, and it is incomplete."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('[tts]\ntts_ws_uri = "wss://tts.example.test:9443"\n')

        with pytest.raises(ConfigError, match="must be set together"):
            load_config(config_file=config_file, env={"WEBSEARCH_TTS_WS_HOST": "127.0.0.1"})

    def test_half_pair_guard_names_the_registry_pair_keys(self) -> None:
        """P3 Phase 1 (program row 13): the half-pair guard derives its key
        names from the registry row (the `host_port` member of
        `_TTS_ENDPOINT_MEMBERS`) instead of hand-written literals. Reading the
        pair from the registry here means a renamed or regrown row makes this
        test fail loudly instead of the guard silently checking stale keys."""
        from server.config import _TTS_ENDPOINT_MEMBERS

        (pair,) = [keys for name, keys in _TTS_ENDPOINT_MEMBERS if name == "host_port"]
        host_key, port_key = pair
        expected = rf"{re.escape(host_key)} and {re.escape(port_key)} must be set together"
        with pytest.raises(ConfigError, match=expected):
            load_config(env={host_key: "127.0.0.1"})

    def test_tts_host_port_builder_reads_the_registry_pairs_key_names(self) -> None:
        """P3 Phase 1 (program row 13) closed the half-pair GUARD's drift
        surface but not the endpoint BUILDER's: `_build_tts_endpoint`'s
        `host_port` branch still reads `values["WEBSEARCH_TTS_WS_HOST"]`/
        `WS_PORT"]` as two hardcoded literals, independent of
        `_TTS_ENDPOINT_MEMBERS`. Building the endpoint using ONLY the
        registry-derived key names pins today's agreement, so a future
        registry rename that the guard tracks but the builder doesn't
        surfaces here as a wrong endpoint instead of staying silently in
        sync by coincidence."""
        from server.config import _TTS_ENDPOINT_MEMBERS, _build_tts_endpoint

        (pair,) = [keys for name, keys in _TTS_ENDPOINT_MEMBERS if name == "host_port"]
        host_key, port_key = pair
        values = {host_key: "127.0.0.1", port_key: "9001"}

        assert _build_tts_endpoint("host_port", values) == ("ws", "127.0.0.1:9001")

    def test_explicit_zero_port_reaches_the_range_validator(self, tmp_path) -> None:
        """Round 6 confirm pass 3, Logic Minor: TOML supplies real integers, so
        a truthiness presence test read an explicit `tts_ws_port = 0` as an
        unset port -- reporting a factually wrong "must be set together"
        instead of the field-named range error."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('[tts]\ntts_ws_host = "127.0.0.1"\ntts_ws_port = 0\n')

        with pytest.raises(ConfigError, match="between 1 and 65535"):
            load_config(config_file=config_file, env={})


class TestAliasFamilyPrecedence:
    """Round 7 confirm pass 4, Logic Minor: the vendor-credential aliases were
    given the endpoint families' layer-first rule in round 6, which let an
    AMBIENT shell `OPENAI_API_KEY`/`DEEPGRAM_API_KEY` -- generic vendor names a
    developer machine carries for unrelated reasons -- silently outrank the
    `WEBSEARCH_`-scoped credential deliberately written into the project's env
    file, and ship the wrong key to a vendor API.

    The endpoint families can take layer-first safely because every one of
    their spellings is `WEBSEARCH_`-prefixed. The alias families' bare
    spellings are deliberately NOT, so the scoped spelling wins at any layer.
    """

    def test_env_file_scoped_voice_id_beats_ambient_process_env_bare_voice_id(self) -> None:
        config = load_config(
            env_file={"WEBSEARCH_CARTESIA_VOICE_ID": "from-env-file"},
            env={"CARTESIA_VOICE_ID": "ambient"},
        )

        assert config.cartesia_voice_id == "from-env-file"

    def test_env_file_scoped_deepgram_key_beats_ambient_process_env_bare_key(self) -> None:
        config = load_config(
            env_file={"WEBSEARCH_DEEPGRAM_API_KEY": "from-env-file"},
            env={"DEEPGRAM_API_KEY": "ambient"},
        )

        assert config.deepgram_api_key == "from-env-file"

    def test_env_file_scoped_openai_key_beats_ambient_process_env_bare_key(self) -> None:
        """The headline case: a developer's shell almost always carries an
        `OPENAI_API_KEY` set for something else entirely."""
        config = load_config(
            env_file={"WEBSEARCH_OPENAI_API_KEY": "from-env-file"},
            env={"OPENAI_API_KEY": "ambient"},
        )

        assert config.openai_api_key == "from-env-file"

    def test_scoped_spelling_wins_a_same_layer_tie(self) -> None:
        config = load_config(
            env={
                "WEBSEARCH_CARTESIA_API_KEY": "scoped",
                "CARTESIA_API_KEY": "bare",
            },
        )

        assert config.cartesia_api_key == "scoped"

    def test_bare_key_is_used_when_the_scoped_spelling_is_unset(self) -> None:
        """The bare spellings exist so an operator can reuse standard vendor
        env vars; scoped-wins must not make them unreachable."""
        config = load_config(env={"DEEPGRAM_API_KEY": "bare-only"})

        assert config.deepgram_api_key == "bare-only"

    def test_empty_scoped_spelling_falls_through_to_the_bare_key(self) -> None:
        """An erased scoped override supplies nothing, so it must not block
        the bare spelling the way a real value does."""
        config = load_config(
            env_file={"WEBSEARCH_DEEPGRAM_API_KEY": "   "},
            env={"DEEPGRAM_API_KEY": "bare"},
        )

        assert config.deepgram_api_key == "bare"

    def test_runtime_named_openai_key_env_participates_in_the_family(self) -> None:
        """The OpenAI family's bare key is named by WEBSEARCH_OPENAI_API_KEY_ENV
        at runtime; it resolves under the same scoped-beats-bare rule."""
        config = load_config(
            env={
                "WEBSEARCH_OPENAI_API_KEY_ENV": "MY_OPENAI_KEY",
                "MY_OPENAI_KEY": "from-redirected-key",
            },
        )

        assert config.openai_api_key == "from-redirected-key"

    def test_scoped_openai_key_beats_the_runtime_named_bare_key(self) -> None:
        config = load_config(
            env_file={"WEBSEARCH_OPENAI_API_KEY": "from-env-file"},
            env={
                "WEBSEARCH_OPENAI_API_KEY_ENV": "MY_OPENAI_KEY",
                "MY_OPENAI_KEY": "ambient",
            },
        )

        assert config.openai_api_key == "from-env-file"

    def test_lower_layer_scoped_key_is_used_when_no_bare_key_is_set(self) -> None:
        config = load_config(
            env_file={"WEBSEARCH_CARTESIA_VOICE_ID": "from-env-file"},
            env={},
        )

        assert config.cartesia_voice_id == "from-env-file"

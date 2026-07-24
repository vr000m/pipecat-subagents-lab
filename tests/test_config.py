"""Contract tests for operator configuration.

These tests deliberately exercise the configuration boundary rather than
environment-specific defaults.  They are test-first: the Phase 1
implementation is expected to provide ``Config`` and ``load_config``.
"""

from pathlib import Path

import pytest

from server.config import Config, ConfigError, load_config


def test_defaults_are_bounded_and_do_not_contain_credentials() -> None:
    config = Config()

    assert config.bind_host == "127.0.0.1"
    assert config.bind_port == 7860
    assert config.known_client_url == "http://127.0.0.1:7860"
    assert config.max_work_items_per_turn == 2
    assert config.multi_intent_wait_timeout_ms == 10_000
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
            "WEBSEARCH_BIND_HOST": "0.0.0.0",
            "WEBSEARCH_BIND_PORT": "9000",
            "WEBSEARCH_KNOWN_CLIENT_URL": "https://example.test:9443/client",
        }
    )

    assert (config.bind_host, config.bind_port, config.known_client_url) == (
        "0.0.0.0",
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
    (("bind_host", ""), ("bind_port", 0), ("bind_port", 65_536), ("known_client_url", "client")),
)
def test_bind_settings_are_validated(field: str, value: object) -> None:
    with pytest.raises(ConfigError):
        Config(**{field: value})


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


def test_tts_uri_precedes_socket_and_host_port(tmp_path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        '[tts]\ntts_ws_uri = "wss://tts.example.test:9443"\n'
        'tts_ws_socket = "~/tts.sock"\ntts_ws_host = "ignored.example.test"\ntts_ws_port = 9000\n'
    )

    config = load_config(config_file=config_file, env={})

    assert config.tts_endpoint == ("wss", "tts.example.test:9443")

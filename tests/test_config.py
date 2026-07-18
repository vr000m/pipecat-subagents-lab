"""Contract tests for operator configuration.

These tests deliberately exercise the configuration boundary rather than
environment-specific defaults.  They are test-first: the Phase 1
implementation is expected to provide ``Config`` and ``load_config``.
"""

import pytest

from server.config import Config, ConfigError, load_config


def test_defaults_are_bounded_and_do_not_contain_credentials() -> None:
    config = Config()

    assert config.bind_host == "127.0.0.1"
    assert config.bind_port == 7860
    assert config.known_client_url == "http://127.0.0.1:7860"
    assert config.max_work_items_per_turn == 2
    assert config.multi_intent_wait_timeout_ms == 10_000
    assert config.openai_api_key is None
    assert config.router_model_policy
    assert config.worker_model_policy


def test_operator_limits_reject_zero_or_unbounded_values() -> None:
    with pytest.raises(ConfigError):
        Config(max_work_items_per_turn=0)

    with pytest.raises(ConfigError):
        Config(max_work_items_per_turn=5)

    with pytest.raises(ConfigError):
        Config(multi_intent_wait_timeout_ms=0)


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

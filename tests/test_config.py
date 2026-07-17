"""Contract tests for operator configuration.

These tests deliberately exercise the configuration boundary rather than
environment-specific defaults.  They are test-first: the Phase 1
implementation is expected to provide ``Config`` and ``load_config``.
"""

import pytest

from server.config import Config, ConfigError, load_config


def test_defaults_are_bounded_and_do_not_contain_credentials() -> None:
    config = Config()

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

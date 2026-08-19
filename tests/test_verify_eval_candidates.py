"""Regression tests for scripts/verify_eval_candidates.py (gauntlet round-2
findings 3 and 5).

Offline-only: run_verification() (which makes real, billed OpenAI calls) is
monkeypatched out everywhere below.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import scripts.verify_eval_candidates as verify_module
from scripts.eval_common import error_text


def _fake_results() -> list[verify_module.ProbeResult]:
    return [
        verify_module.ProbeResult(
            "model_existence", "gpt-5-mini", None, [], {"model": "gpt-5-mini"}, True, None
        )
    ]


class TestManifestWriteOSErrorHandling:
    """Regression for finding 3: write_manifest()'s OSError path (a target
    that's a directory, unwritable, or a symlink -- write_no_follow()'s
    O_NOFOLLOW raising ELOOP) previously wasn't caught at all -- main() only
    caught ValueError -- so an operator lost every already-billed probe
    result with zero output. Fixed by (a) widening the except clause to catch
    OSError too, and (b) printing the summary before attempting the write, so
    the operator sees the results regardless of what the write does next.
    """

    def test_directory_output_path_prints_summary_and_returns_error_code(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        out_dir = tmp_path / "a-directory"
        out_dir.mkdir()

        monkeypatch.setattr(
            verify_module, "run_verification", lambda *, judge_model: (_fake_results(), 12.0)
        )
        # confined_output_path() only allows paths inside the repo root;
        # bypassed here (returns the path unchanged) so this test exercises
        # write_manifest()'s OSError path specifically, not the unrelated
        # ValueError a tmp_path-outside-the-repo target would raise instead.
        monkeypatch.setattr(verify_module, "confined_output_path", lambda p, **_k: Path(p))
        monkeypatch.setattr("sys.argv", ["verify_eval_candidates.py", "--out", str(out_dir)])

        exit_code = verify_module.main()

        assert exit_code == 2
        out = capsys.readouterr()
        # The probe summary must have printed before the write attempt --
        # not just "an error happened with no results shown".
        assert "gpt-5-mini" in out.out
        assert "probes accepted" in out.out

    def test_write_no_follow_raises_oserror_for_a_directory_target(self, tmp_path: Path) -> None:
        # The underlying write path write_manifest() delegates to -- a
        # direct-call regression proving the OSError main() now catches is
        # real, independent of main()'s argv wiring.
        from scripts.eval_common import write_no_follow

        out_dir = tmp_path / "another-directory"
        out_dir.mkdir()
        with pytest.raises(OSError):
            write_no_follow(out_dir, "content")


class TestNonOpenAIShapedCredentialRedaction:
    """Regression for finding 5: the sk-... pattern match is bypassable for
    any non-OpenAI-shaped credential (Azure OpenAI key, gateway/proxy token,
    etc. -- WEBSEARCH_OPENAI_API_KEY_ENV explicitly supports pointing at one).
    error_text() now also does an exact substring replace on the resolved
    credential, which is shape-independent.
    """

    def test_hex_shaped_credential_is_redacted_via_exact_match(self) -> None:
        fake_credential = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"  # 40-char hex, not sk-...
        exc = RuntimeError(f"upstream rejected token {fake_credential}")

        text = error_text(exc, credential=fake_credential)

        assert fake_credential not in text
        assert "REDACTED" in text

    def test_sk_shaped_credential_still_redacted_via_pattern_fallback(self) -> None:
        # No credential passed at the call site -- must still fall back to
        # the pattern match for OpenAI's own key shape.
        exc = RuntimeError("upstream rejected token sk-abcdefghijklmnopqrstuvwxyz")

        text = error_text(exc, credential=None)

        assert "sk-abcdefghijklmnopqrstuvwxyz" not in text
        assert "REDACTED" in text

    def test_probes_thread_the_resolved_api_key_into_error_text(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_credential = "proxy-token-not-sk-shaped-0123456789"

        class _RaisingModelsClient:
            def retrieve(self, _model: str) -> None:
                raise RuntimeError(f"rejected: {fake_credential}")

        result = verify_module.probe_model_existence(
            _RaisingModelsClient(), "gpt-5-mini", api_key=fake_credential
        )

        assert result.accepted is False
        assert fake_credential not in (result.error or "")


class TestProbeBuildersCallProductionsOwnSharedBuilder:
    """Regression for round 8 gauntlet, Architecture finding 8: the probe's
    request-kwargs shape was previously a hand-mirrored second copy of
    ``LazyRouterProvider.__call__``/``WebSearchWorker.search``'s payloads --
    a production kwarg change left the manifest verifier free to keep
    reproducing the OLD shape and re-certify it as still
    production-equivalent. ``_build_router_kwargs``/``_build_worker_kwargs``
    now call production's own ``build_router_request_kwargs``/
    ``build_worker_request_kwargs`` directly, so this asserts identity with
    what production would build for the same inputs -- not merely "the same
    shape by inspection".
    """

    def test_router_probe_kwargs_are_identical_to_productions_own_builder_output(self) -> None:
        from server.router import WorkerCatalogue, build_router_request_kwargs

        catalogue = WorkerCatalogue(
            version="verify-eval-candidates-1",
            workers=(),
            capability_labels=("public_web",),
            model_policies=("deep",),
        )
        prompt = catalogue.prompt(verify_module.ROUTER_PROBE_TRANSCRIPT)
        expected = build_router_request_kwargs("gpt-5-mini", "medium", prompt=prompt, timeout=7.5)

        actual = verify_module._build_router_kwargs(
            "gpt-5-mini", "medium", router_timeout_seconds=7.5
        )

        assert actual == expected

    def test_worker_probe_kwargs_are_identical_to_productions_own_builder_output(self) -> None:
        from server.workers.web_search import build_worker_request_kwargs

        expected = build_worker_request_kwargs(
            "gpt-5", "high", query=verify_module.WORKER_PROBE_QUERY
        )

        actual = verify_module._build_worker_kwargs("gpt-5", "high")

        assert actual == expected

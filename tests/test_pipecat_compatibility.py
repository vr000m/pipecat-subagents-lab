"""Versioned compatibility assertions for the pinned Pipecat worker seams."""

from importlib import import_module
from importlib.metadata import version

import pytest

from pipecat.workers.base_worker import BaseWorker
from server.workers.base import ContextWorker


_PINNED_PIPECAT_VERSION = "1.8.0"
_LLM_CONTEXT_WORKER_MODULE = "pipecat.workers.llm.llm_context_worker"
_OLD_LLM_CONTEXT_WORKER_MODULE = "pipecat.processors.frameworks.llm_context"


def test_pipecat_1_8_exports_llm_context_worker_and_app_uses_base_worker_seam() -> None:
    """Fail loudly when the pinned import contract or deliberate seam changes."""
    installed_version = version("pipecat-ai")
    assert installed_version == _PINNED_PIPECAT_VERSION, (
        f"Pipecat changed to {installed_version}; re-verify the versioned "
        f"LLMContextWorker import before updating this test"
    )

    module = import_module(_LLM_CONTEXT_WORKER_MODULE)
    native_worker = getattr(module, "LLMContextWorker")
    assert native_worker.__module__ == _LLM_CONTEXT_WORKER_MODULE

    with pytest.raises(ImportError):
        import_module(_OLD_LLM_CONTEXT_WORKER_MODULE)

    assert ContextWorker.__bases__ == (BaseWorker,)

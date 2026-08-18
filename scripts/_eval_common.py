"""Backward-compatible re-export shim.

This module moved to ``scripts/eval_common.py`` in round 5 of the review
gauntlet (Architecture lens finding 6: a leading-underscore, package-private
name was being used as a public seam by three separate consumers). New code
should import from ``scripts.eval_common`` directly; this shim exists only so
callers (and tests) still importing ``scripts._eval_common`` keep working
unchanged.

Explicit re-exports (not ``from scripts.eval_common import *``) so ruff's
F403/F401 checks stay meaningful for this file rather than needing a blanket
suppression. Each line is intentionally `X as X` -- that is the re-export
idiom itself, not an accidental rename, so PLC0414 is suppressed per line
rather than restructured away.
"""

from __future__ import annotations

from scripts.eval_common import (
    DEFAULT_JUDGE_MODEL as DEFAULT_JUDGE_MODEL,  # noqa: PLC0414 - intentional backward-compat re-export
)
from scripts.eval_common import (
    DEFAULT_MANIFEST_RELATIVE_PATH as DEFAULT_MANIFEST_RELATIVE_PATH,
)
from scripts.eval_common import (
    DEFAULT_QUERY as DEFAULT_QUERY,  # noqa: PLC0414 - intentional backward-compat re-export
)
from scripts.eval_common import (
    MANIFEST_VERSION as MANIFEST_VERSION,  # noqa: PLC0414 - intentional backward-compat re-export
)
from scripts.eval_common import (
    ROUTER_MANIFEST_TOOLS as ROUTER_MANIFEST_TOOLS,
)
from scripts.eval_common import (
    ROUTING_REGRESSION_QUERIES as ROUTING_REGRESSION_QUERIES,
)
from scripts.eval_common import (
    SAFE_FALLBACKS as SAFE_FALLBACKS,  # noqa: PLC0414 - intentional backward-compat re-export
)
from scripts.eval_common import (
    TIMEOUT_FALLBACKS as TIMEOUT_FALLBACKS,  # noqa: PLC0414 - intentional backward-compat re-export
)
from scripts.eval_common import (
    WORKER_MANIFEST_TOOLS as WORKER_MANIFEST_TOOLS,
)
from scripts.eval_common import (
    CollectingMeasurementSink as CollectingMeasurementSink,
)
from scripts.eval_common import (
    _latest_turn_stage_metrics as _latest_turn_stage_metrics,
)
from scripts.eval_common import (
    build_judge_llm_service as build_judge_llm_service,
)
from scripts.eval_common import (
    build_session_for_run as build_session_for_run,
)
from scripts.eval_common import (
    confined_output_path as confined_output_path,
)
from scripts.eval_common import (
    error_text as error_text,  # noqa: PLC0414 - intentional backward-compat re-export
)
from scripts.eval_common import (
    latest_turn_stage_metrics as latest_turn_stage_metrics,
)
from scripts.eval_common import (
    strip_control_chars as strip_control_chars,
)
from scripts.eval_common import (
    write_no_follow as write_no_follow,  # noqa: PLC0414 - intentional backward-compat re-export
)

__all__ = [
    "DEFAULT_JUDGE_MODEL",
    "DEFAULT_MANIFEST_RELATIVE_PATH",
    "DEFAULT_QUERY",
    "MANIFEST_VERSION",
    "ROUTER_MANIFEST_TOOLS",
    "ROUTING_REGRESSION_QUERIES",
    "SAFE_FALLBACKS",
    "TIMEOUT_FALLBACKS",
    "WORKER_MANIFEST_TOOLS",
    "CollectingMeasurementSink",
    "build_judge_llm_service",
    "build_session_for_run",
    "confined_output_path",
    "error_text",
    "latest_turn_stage_metrics",
    "strip_control_chars",
    "write_no_follow",
]

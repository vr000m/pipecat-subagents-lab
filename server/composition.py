"""Single composition root for wiring a ``SessionHost`` and its collaborators.

Before round 5 of the review gauntlet, ``server/app.py``'s
``_default_session_host()`` and ``scripts/eval_common.py``'s
``build_session_for_run()`` independently constructed the same
``WorkerRegistry -> Router(LazyRouterProvider) -> WorkItemCoordinator ->
SessionHost`` object graph, with nothing detecting divergence between the
two. They had already drifted (production omitted ``config=`` to
``SessionHost``; the eval builder passed it -- functionally harmless, since
``SessionHost.__init__`` falls back to the registry's own config, but a
divergence with no guard against a future one that isn't harmless). Any
component added to this graph now needs to be added here once, not in two
independently-maintained call sites (Architecture lens finding 1).

``server.app._default_session_host()`` and ``scripts.eval_common.build_session_for_run()``
both delegate to :func:`build_session_host` below -- see each for the
production-only (stt/tts/promotion-manifest resolution) or eval-only
(measurement sink, canned responses factories) concerns layered on top.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .config import Config, PromotionManifest
from .perf_metrics import MeasurementSink
from .pipeline import SessionHost
from .registry import WorkerRegistry
from .router import LazyRouterProvider, Router
from .work_item_coordinator import WorkItemCoordinator


def build_session_host(
    config: Config,
    *,
    router: Router | None = None,
    router_responses_factory: Callable[[], Any] | None = None,
    worker_responses: Any = None,
    measurement_sink: MeasurementSink | None = None,
    promotion_manifest: PromotionManifest | None = None,
    stt: Any | None = None,
    tts: Any | None = None,
) -> SessionHost:
    """Construct a fresh ``WorkerRegistry``/``Router``/``WorkItemCoordinator``/
    ``SessionHost``, all bound to this exact ``config`` at construction time.

    ``router``, when supplied, is used as-is (the caller has already bound it
    to whatever ``Config`` it needs) -- this is the seam
    ``server.app._default_session_host(router=...)`` uses for tests that
    inject a fake router. When omitted, a ``Router(call=LazyRouterProvider(config,
    router_responses_factory), config=config)`` is built here, bound to
    *this* ``config``: ``LazyRouterProvider`` captures its ``Config``
    reference at construction and resolves the model/effort from that
    captured reference at call time, so a caller that needs to vary the
    router's resolved model/effort per run (the eval-suite runner) must go
    through this construction path -- a post-hoc ``host.config = tuned``
    reassignment never reaches the router provider's own captured reference.

    ``promotion_manifest`` is threaded straight through to
    ``SessionHost(..., promotion_manifest=...)``: omitting it leaves
    ``SessionHost._promotion_eligible`` permanently ``False`` (fail-closed to
    the ``"display_only"`` late-delivery disposition regardless of the
    caller's actual manifest), which is correct for a caller that has no
    manifest to give but silently wrong for one that does. Production's own
    ``server.app._default_session_host()`` resolves one via
    ``load_promotion_manifest(config)`` before calling here; a caller
    exercising the promotion-eligible path (e.g. the ack-ordering smoke, or a
    future eval scenario) must resolve and pass its own manifest through the
    same way -- this composition root does not resolve one itself, since not
    every caller (most eval/smoke scenarios) needs the promotion-eligible path
    at all (round 7 gauntlet, Architecture finding 16 -- rationale moved here
    from ``scripts/eval_common.py``'s ``build_session_for_run``, this
    function's eval-only wrapper, which previously duplicated it).

    ``tts``, like ``router`` above, is captured at construction time only:
    ``SessionHost.__init__`` binds it into ``self._tts_on_event``, a
    constructor-captured reference a post-hoc ``host.tts = ...``
    reassignment can't reach -- the same failure shape as the router
    config-capture note above. A caller that needs a specific TTS stand-in
    (e.g. ``scripts/smoke_conversation.py``'s ``_RecordingTTS``, for the
    ack-ordering scenario) must pass it through this constructor's ``tts=``
    parameter, not assign it after the fact (round 8 gauntlet, Architecture
    finding 13 -- restored here after round 7's doc-consolidation moved the
    composition rationale into this docstring but dropped this note).
    """
    registry = WorkerRegistry(config=config, responses=worker_responses)
    configured_router = router or Router(
        call=LazyRouterProvider(config, router_responses_factory),
        config=config,
    )
    coordinator = WorkItemCoordinator(registry=registry, router=configured_router, config=config)
    return SessionHost(
        registry=registry,
        stt=stt,
        tts=tts,
        coordinator=coordinator,
        measurement_sink=measurement_sink,
        config=config,
        promotion_manifest=promotion_manifest,
    )

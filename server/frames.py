"""Private frame types shared across connection modules.

Home for frames that no single feature module owns, and for
``CONNECTION_LOCAL_FRAMES``, the bus-bridge exclusion registry that lists
every connection-local frame type regardless of which module defines it.
``speech_lifecycle`` owns the speech-generation frame types themselves and
imports nothing back from here, so this module can safely reference them for
the registry without a circular import.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pipecat.frames.frames import Frame, SystemFrame, TTSSpeakFrame

from .speech_lifecycle import SpeechGenerationFlushAckFrame, SpeechGenerationMarkerFrame


@dataclass
class SnapshotBarrierFlushFrame(SystemFrame):
    """Serialized-writer barrier ordering the Phase 3 snapshot handoff.

    Carries only a connection-generation/token and an acknowledgement
    handle; `server/app.py` is its sole consumer, resolving the handle only
    after every earlier queued worker frame has drained so no incremental
    RTVI event can reach the network ahead of the installed snapshot
    watermark. It is never an RTVI payload and must never cross a bus
    bridge, hence its membership in ``CONNECTION_LOCAL_FRAMES`` (below).
    """

    token: str = ""
    acknowledge: Any = None


CONNECTION_LOCAL_FRAMES: tuple[type[Frame], ...] = (
    TTSSpeakFrame,
    SpeechGenerationMarkerFrame,
    SpeechGenerationFlushAckFrame,
    SnapshotBarrierFlushFrame,
)
"""Frame types that must never cross the bus bridge (``BusBridgeProcessor``).

``BusBridgeProcessor`` forwards a frame downstream locally only if it is a
lifecycle frame, an ``OutputTransportMessageUrgentFrame``, or explicitly
listed in ``exclude_frames``; every other frame type is diverted to the bus
and never reaches the rest of the connection pipeline. A connection-local
frame omitted from this tuple is silently dropped from the local pipeline
with no error or log (see docs/architecture.md, "Bus bridge frame
exclusions").

Any new frame type that must stay connection-local rather than cross the bus
bridge must be added here at the moment it is defined.
"""

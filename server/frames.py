"""Private frame types shared across connection modules.

Home for frames that no single feature module owns. ``speech_lifecycle`` owns
the speech-generation frames and ``observers`` owns projection; a frame both
must reference lives here so neither imports the other's vocabulary.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pipecat.frames.frames import SystemFrame


@dataclass
class SnapshotBarrierFlushFrame(SystemFrame):
    """Serialized-writer barrier ordering the Phase 3 snapshot handoff.

    Carries only a connection-generation/token and an acknowledgement
    handle; `server/app.py` is its sole consumer, resolving the handle only
    after every earlier queued worker frame has drained so no incremental
    RTVI event can reach the network ahead of the installed snapshot
    watermark. It is never an RTVI payload and must never cross a bus
    bridge, hence its membership in ``CONNECTION_LOCAL_FRAMES``
    (`server/speech_lifecycle.py`).
    """

    token: str = ""
    acknowledge: Any = None

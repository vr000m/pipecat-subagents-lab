"""Browser-handshake resume-token issuance/validation owned by a SessionHost.

Extracted from SessionHost (deep-review Architecture finding #1: SessionHost
god class) as the lowest-risk slice of the decomposition -- token storage,
pruning, and validation are pure (no ``asyncio``, no scheduler) and have no
coupling to any other SessionHost state. ``validate_patch_handshake`` moved
alongside them because it is the other half of "is this handshake legit",
even though it does not touch the token store itself.

Session-identity data (``session_id``, ``sequence``) and epoch numbering
(``ConnectionArbiter.epoch``) stay on SessionHost -- this class only mints
and checks the opaque resume tokens; ``SessionHost.session_handshake``
composes the full handshake payload around ``issue_token``'s return value.

This module is a pure extraction of SessionHost's existing storage and
validation logic -- semantics, including comparison/expiry/pruning order,
are unchanged from the code that previously lived directly on SessionHost.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from .connection_arbiter import Connection
    from .contracts import SnapshotHandshake
    from .pipeline import ConnectionPipeline


class HandshakeGate:
    """Tracks and validates a SessionHost's outstanding resume-token handshakes.

    Each stored token maps to ``(epoch, expires_at, redeemed)``: the epoch
    the token was minted for, its ``time.monotonic()`` expiry, and whether it
    has already been redeemed. Tokens are pruned lazily (on every issue/
    validate call) rather than on a timer, and the store is capped at
    ``_MAX_HANDSHAKE_TOKENS`` entries, evicting the token with the nearest
    expiry once the cap is exceeded.
    """

    _MAX_HANDSHAKE_TOKENS = 32

    def __init__(self) -> None:
        self._handshake_tokens: dict[str, tuple[int, float, bool]] = {}

    def issue_token(self, epoch: int) -> str:
        """Mint and store a fresh resume token bound to ``epoch``.

        Prunes expired tokens first, then evicts the token nearest expiry
        (repeatedly) if the store is still over capacity after the new
        token is added.
        """
        self._prune_handshake_tokens()
        token = uuid4().hex
        self._handshake_tokens[token] = (epoch, time.monotonic() + 60, False)
        while len(self._handshake_tokens) > self._MAX_HANDSHAKE_TOKENS:
            oldest = min(self._handshake_tokens, key=lambda item: self._handshake_tokens[item][1])
            self._handshake_tokens.pop(oldest, None)
        return token

    def validate_handshake_token(self, token: str, proposed_epoch: int, *, redeem: bool) -> bool:
        self._prune_handshake_tokens()
        entry = self._handshake_tokens.get(token)
        if entry is None:
            return False
        epoch, expires_at, redeemed = entry
        if expires_at <= time.monotonic() or epoch != proposed_epoch:
            self._handshake_tokens.pop(token, None)
            return False
        if redeem:
            if redeemed:
                return False
            self._handshake_tokens[token] = (epoch, expires_at, True)
            return True
        return redeemed

    def validate_patch_handshake(
        self,
        connection: Connection | ConnectionPipeline | None,
        handshake: SnapshotHandshake,
    ) -> None:
        """Enforce immutable capability binding for a PATCH ICE-candidate request.

        An omitted ``capabilities`` field inherits the POST-bound set; a
        present field must match it exactly (post-normalization) or the
        request is rejected. Neither branch mutates ``connection`` or its
        already-constructed ``RuntimeObserver``: capability entitlement is
        immutable for the life of a promoted epoch (Requirements).

        Both concrete carriers are accepted because both real call sites are
        typed: ``server.app`` passes the live ``ConnectionPipeline`` (whose
        ``capabilities`` property reads straight off its ``RuntimeObserver``)
        while the arbiter-level tests pass the promoted ``Connection``. A
        ``None`` connection means nothing was promoted, so the bound set is
        empty and any presented capability is a mismatch. Attributes are read
        directly rather than via ``getattr`` defaults, so a wrong argument or
        a future field rename raises instead of silently degrading to "no
        capabilities presented".
        """
        if not handshake.capabilities_present:
            return
        bound = frozenset(connection.capabilities) if connection is not None else frozenset()
        presented = frozenset(handshake.capabilities)
        if presented != bound:
            raise ValueError("capabilities cannot change after connection promotion")

    def _prune_handshake_tokens(self) -> None:
        now = time.monotonic()
        expired = [
            token
            for token, (_epoch, expires_at, _redeemed) in self._handshake_tokens.items()
            if expires_at <= now
        ]
        for token in expired:
            self._handshake_tokens.pop(token, None)

"""Application entry seam for the browser RTVI server."""

from __future__ import annotations

from .pipeline import SessionHost


def create_app(host: SessionHost | None = None) -> SessionHost:
    """Return the durable host used by the HTTP/Small-WebRTC integration."""
    return host or SessionHost()


async def serve(host: SessionHost | None = None) -> SessionHost:
    runtime = create_app(host)
    await runtime.start()
    return runtime

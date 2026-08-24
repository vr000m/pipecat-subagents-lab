"""Repository maintenance and evidence-gate scripts.

This package exists so the scripts can share helpers (``evidence_common``)
through ordinary package-qualified imports instead of mutating ``sys.path``.
Direct invocation (``uv run python scripts/foo.py``) keeps working because the
project is installed into the environment, which puts the repository root on
``sys.path``.
"""

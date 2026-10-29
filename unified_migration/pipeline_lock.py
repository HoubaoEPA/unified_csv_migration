"""PostgreSQL advisory lock shared by migration workflows."""

from __future__ import annotations

import hashlib
import os
import time
from contextlib import contextmanager
from typing import Iterator

import psycopg


LOCK_NAME = "chemdash:data-pipeline"
LOCK_ID = int.from_bytes(
    hashlib.sha256(LOCK_NAME.encode("utf-8")).digest()[:8],
    byteorder="big",
    signed=True,
)
DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_POLL_SECONDS = 0.25


class PipelineLockTimeout(TimeoutError):
    """Raised when another migration or publication owns the lock."""


def _psycopg_url(database_url: str) -> str:
    return database_url.replace("postgresql+psycopg://", "postgresql://", 1)


def lock_timeout_seconds() -> float:
    raw = os.getenv("CHEMDASH_PIPELINE_LOCK_TIMEOUT_SECONDS")
    if raw is None:
        return DEFAULT_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(
            "CHEMDASH_PIPELINE_LOCK_TIMEOUT_SECONDS must be a non-negative number"
        ) from exc
    if value < 0:
        raise ValueError(
            "CHEMDASH_PIPELINE_LOCK_TIMEOUT_SECONDS must be a non-negative number"
        )
    return value


@contextmanager
def pipeline_lock(
    database_url: str,
    *,
    timeout_seconds: float | None = None,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
) -> Iterator[None]:
    """Hold a session-level lock for one complete data-changing workflow."""
    timeout = lock_timeout_seconds() if timeout_seconds is None else timeout_seconds
    if timeout < 0:
        raise ValueError("timeout_seconds must be non-negative")
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")

    with psycopg.connect(_psycopg_url(database_url), autocommit=True) as connection:
        deadline = time.monotonic() + timeout
        while True:
            acquired = connection.execute(
                "SELECT pg_try_advisory_lock(%s)", (LOCK_ID,)
            ).fetchone()[0]
            if acquired:
                break
            if time.monotonic() >= deadline:
                raise PipelineLockTimeout(
                    f"Timed out waiting for the data-pipeline lock after {timeout:g} "
                    "seconds. Another migration or publication is still running."
                )
            time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))

        try:
            yield
        finally:
            connection.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))

"""Capped backoff sleep between retries (Testflows-aware).

Standalone module so kubectl/util do not circular-import each other when importing
``retry_sleep``.
"""

import time

from testflows.core import Then

# Upper bound on linear backoff between retry attempts (see `retry_sleep`).
_max_retry_delay_sec = 30


def retry_sleep(attempt_index, step_sec, reason=None):
    """Compute capped backoff, emit Testflows `Then(...)`, and sleep.

    ``reason`` is a short explanation of why we are waiting (no duration suffix).
    It is followed automatically by ``Wait for <n> seconds``. If ``reason`` is None
    or whitespace-only, only the wait line is shown.
    """
    delay = min(_max_retry_delay_sec, attempt_index * step_sec)
    reason_str = "" if reason is None else str(reason).strip()
    if not reason_str:
        msg = f"Wait for {delay} seconds"
    elif reason_str[-1] in ".!?":
        msg = f"{reason_str} Wait for {delay} seconds"
    else:
        msg = f"{reason_str}. Wait for {delay} seconds"

    with Then(msg):
        time.sleep(delay)

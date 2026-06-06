"""
Timezone helpers for the app.

Storage convention: all datetimes stored in MongoDB are naive UTC
(no tzinfo), matching what datetime.utcnow() produced.  These helpers
preserve that convention while fixing the DeprecationWarning.

Usage:
    from helpers.datetime_utils import utcnow, to_ist, IST

    created_at = utcnow()          # naive UTC — safe to store in Mongo
    display    = to_ist(created_at)  # aware IST — use only in response bodies / display
"""

from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))


def utcnow() -> datetime:
    """Return current time as a naive UTC datetime (drop-in for datetime.utcnow())."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_ist(dt: datetime) -> datetime:
    """
    Convert a naive UTC datetime (as stored in MongoDB) to an aware IST datetime.
    Safe to call on already-aware datetimes — they are converted correctly.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST)


def to_ist_str(dt: datetime, fmt: str = "%d %b %Y, %I:%M %p IST") -> str:
    """Format a UTC datetime as a human-readable IST string."""
    return to_ist(dt).strftime(fmt)

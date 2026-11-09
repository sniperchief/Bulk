"""Errors Bulk reports to the user as plain text instead of a traceback."""

from __future__ import annotations


class BulkError(Exception):
    """Base class for expected, user-reportable failures."""


class UsageError(BulkError):
    """The slash command arguments were invalid."""


class DiscoveryError(BulkError):
    """The source could not be read or contains no processable items."""


class JobNotFound(BulkError):
    """No job exists with the requested id."""


class ItemProcessingError(BulkError):
    """One item could not be processed; the job itself can continue."""


class EmptyModelResponse(ItemProcessingError):
    """The model returned no text. Treated as a transient failure."""


class StoreError(BulkError):
    """The database refused a state transition or is unusable by this version."""


class JobStateError(BulkError):
    """The job's current state does not allow the requested action."""


class JobBlocked(BulkError):
    """No item may be claimed because the job is paused or at its token limit."""


class ShuttingDown(BulkError):
    """Bulk is stopping, so an item between retries gives up its claim."""

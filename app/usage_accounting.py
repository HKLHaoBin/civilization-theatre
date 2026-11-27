"""Bounded, sanitized request-usage journal.

The journal records request attempts, not provider billing confirmation. The
request identity is explicit so a repeated completion can be ignored and a
conflicting duplicate can fail closed instead of silently double-counting.
"""
import json
import os
from pathlib import Path
import re
import threading

MAX_JOURNAL_BYTES = 4 * 1024 * 1024
MAX_LINE_BYTES = 4096
MAX_REQUEST_INDEX = 4
_EVENTS = frozenset({'request_started', 'request_returned', 'request_failed'})
_ID = re.compile(r'[A-Za-z0-9._-]{1,128}\Z')


class UsageJournalError(ValueError):
    pass


def _bounded_integer(value, *, minimum=0):
    return type(value) is int and minimum <= value <= 2**63 - 1


def _validate(event):
    fields = {'event', 'kind', 'request_id', 'request_index',
              'input_tokens', 'output_tokens', 'cost', 'billing_status'}
    if type(event) is not dict or set(event) != fields:
        raise UsageJournalError('INVALID_USAGE_EVENT')
    if event['event'] not in _EVENTS or event['kind'] not in ('roles', 'dm'):
        raise UsageJournalError('INVALID_USAGE_EVENT')
    if type(event['request_id']) is not str or not _ID.fullmatch(event['request_id']):
        raise UsageJournalError('INVALID_USAGE_EVENT')
    if type(event['request_index']) is not int or not 1 <= event['request_index'] <= MAX_REQUEST_INDEX:
        raise UsageJournalError('INVALID_USAGE_EVENT')
    for key in ('input_tokens', 'output_tokens'):
        value = event[key]
        if value is not None and not _bounded_integer(value):
            raise UsageJournalError('INVALID_USAGE_EVENT')
    # Cost is intentionally unknown until an independently authorized billing
    # receipt exists. Never turn an unknown cost into zero.
    if event['cost'] is not None or event['billing_status'] != 'UNKNOWN':
        raise UsageJournalError('INVALID_USAGE_EVENT')
    return json.loads(json.dumps(event, sort_keys=True, allow_nan=False))


def _identity(event):
    return (event['event'], event['kind'], event['request_id'],
            event['request_index'])


class UsageJournal:
    """One-process writer with readback deduplication and a bounded file."""
    def __init__(self, path, *, max_bytes=MAX_JOURNAL_BYTES):
        self.path = Path(path)
        self.max_bytes = max_bytes
        self.lock = threading.RLock()
        self._loaded = False
        self._events = {}

    def _load(self):
        if self._loaded:
            return
        if self.path.is_symlink() or self.path.exists() and self.path.stat().st_size > self.max_bytes:
            raise UsageJournalError('INVALID_USAGE_FILE')
        if self.path.exists():
            try:
                with self.path.open('r', encoding='utf-8', newline='') as stream:
                    for raw in stream:
                        if len(raw.encode('utf-8')) > MAX_LINE_BYTES:
                            raise UsageJournalError('INVALID_USAGE_FILE')
                        event = _validate(json.loads(raw))
                        key = _identity(event)
                        previous = self._events.get(key)
                        if previous is not None and previous != event:
                            raise UsageJournalError('USAGE_EVENT_CONFLICT')
                        self._events[key] = event
            except UsageJournalError:
                raise
            except (OSError, ValueError, TypeError, UnicodeError, RecursionError):
                raise UsageJournalError('INVALID_USAGE_FILE') from None
        self._loaded = True

    def append(self, event):
        event = _validate(event)
        key = _identity(event)
        encoded = (json.dumps(event, sort_keys=True, ensure_ascii=False,
                              allow_nan=False, separators=(',', ':')) + '\n').encode('utf-8')
        if len(encoded) > MAX_LINE_BYTES:
            raise UsageJournalError('INVALID_USAGE_EVENT')
        with self.lock:
            self._load()
            previous = self._events.get(key)
            if previous is not None:
                if previous != event:
                    raise UsageJournalError('USAGE_EVENT_CONFLICT')
                return {'written': False, 'duplicate': True, 'identity': key}
            if self.path.exists() and self.path.stat().st_size + len(encoded) > self.max_bytes:
                raise UsageJournalError('USAGE_FILE_LIMIT')
            self.path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with self.path.open('ab') as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
            except OSError:
                raise UsageJournalError('USAGE_WRITE_FAILED') from None
            self._events[key] = event
            return {'written': True, 'duplicate': False, 'identity': key}

    def rows(self):
        with self.lock:
            self._load()
            return [dict(value) for value in self._events.values()]


__all__ = ['MAX_JOURNAL_BYTES', 'UsageJournal', 'UsageJournalError']

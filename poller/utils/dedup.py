"""
Deduplication state manager.

Strategy: ETag (HTTP conditional request) + last_event_id (in-memory filter)

- ETag    : GitHub returns 304 Not Modified when nothing changed → zero-parse cost.
- last_event_id : When content *has* changed, filter out already-seen events
                  by stopping at the first known event ID.

State is persisted to a JSON file so the poller survives container restarts.
"""

import json
import os
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

_DEFAULT_STATE_FILE = os.getenv("STATE_FILE", "/tmp/poller_state.json")


class EventState:
    """Persist and expose ETag + last_event_id across poll cycles."""

    def __init__(self, state_file: str = _DEFAULT_STATE_FILE) -> None:
        self._path = state_file
        self._data: dict = self._load()

    # ------------------------------------------------------------------ #
    # Persistence                                                          #
    # ------------------------------------------------------------------ #

    def _load(self) -> dict:
        if os.path.exists(self._path):
            try:
                with open(self._path) as fh:
                    return json.load(fh)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Could not load state file (%s). Starting fresh.", exc)
        return {"etag": None, "last_event_id": None}

    def _persist(self) -> None:
        try:
            with open(self._path, "w") as fh:
                json.dump(self._data, fh)
        except OSError as exc:
            logger.error("Could not save state file: %s", exc)

    # ------------------------------------------------------------------ #
    # Properties                                                           #
    # ------------------------------------------------------------------ #

    @property
    def etag(self) -> Optional[str]:
        return self._data.get("etag")

    @etag.setter
    def etag(self, value: Optional[str]) -> None:
        self._data["etag"] = value
        self._persist()

    @property
    def last_event_id(self) -> Optional[str]:
        return self._data.get("last_event_id")

    @last_event_id.setter
    def last_event_id(self, value: Optional[str]) -> None:
        self._data["last_event_id"] = value
        self._persist()

    # ------------------------------------------------------------------ #
    # Filtering                                                            #
    # ------------------------------------------------------------------ #

    def filter_new_events(self, events: List[dict]) -> List[dict]:
        """
        Return only events newer than *last_event_id*.

        GitHub returns events in reverse-chronological order (newest first),
        so we collect items until we encounter the last-known event ID.
        """
        if not self.last_event_id:
            return events

        new: List[dict] = []
        for event in events:
            if str(event.get("id")) == str(self.last_event_id):
                break
            new.append(event)

        return new

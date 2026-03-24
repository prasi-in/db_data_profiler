"""Persistence helpers for incremental output and resume behavior."""

from __future__ import annotations

import dataclasses
import json
import threading
from pathlib import Path

from .models import TableProfile
from .utils import json_default


class JsonlStateStore:
    """Append-only JSONL state store used for resumable profiling."""

    def __init__(self, output_path: str):
        """Initialize the state store and load already-completed keys."""
        self.output_path = Path(output_path)
        self.lock = threading.Lock()
        self.completed_keys = self._load_completed_keys()

    def _load_completed_keys(self) -> set[str]:
        """Read completed table keys from an existing JSONL output file."""
        if not self.output_path.exists():
            return set()

        keys: set[str] = set()
        with self.output_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                    key = payload.get("table_key")
                    if key:
                        keys.add(key)
                except Exception:
                    continue
        return keys

    def is_complete(self, table_key: str) -> bool:
        """Return whether the table has already been profiled."""
        return table_key in self.completed_keys

    def append(self, table_key: str, profile: TableProfile) -> None:
        """Append a completed profile to the JSONL output file."""
        row = {"table_key": table_key, "profile": dataclasses.asdict(profile)}
        with self.lock:
            with self.output_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, default=json_default) + "\n")
            self.completed_keys.add(table_key)

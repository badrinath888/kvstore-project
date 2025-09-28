#!/usr/bin/env python3
# KV Store Project 1 - Simple Append-Only Key-Value Store
# Author: Badrinath | EUID: 11820168

import sys
import os
from typing import List, Tuple, Optional

DATA_FILE = "data.db"

class KeyValueStore:
    """Append-only persistent key-value store with a linear in-memory index (no dict/map)."""

    def __init__(self) -> None:
        """Initialize and rebuild index by replaying the log if present."""
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """Replay append-only log into memory; ignore malformed lines safely."""
        if not os.path.exists(DATA_FILE):
            return
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    self._set_in_memory(key, value)

    def _set_in_memory(self, key: str, value: str) -> None:
        """Overwrite existing key or append new pair (O(n))."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """Durably append the SET record then update in-memory view."""
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
            f.write(f"SET {safe_key} {safe_value}\n")
            f.flush()
            os.fsync(f.fileno())
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """Return latest value for key, or None if not present."""
        for k, v in self.index:
            if k == key:
                return v
        return None

def main() -> None:
    """CLI: reads from STDIN, writes to STDOUT. Commands: SET, GET, EXIT."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    store = KeyValueStore()
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        parts = line.split(maxsplit=2)
        cmd = parts[0].upper() if parts else ""

        if cmd == "EXIT":
            break
        elif cmd == "SET":
            if len(parts) != 3:
                sys.stdout.buffer.write(b"ERR wrong number of arguments for 'SET'\n")
                sys.stdout.flush()
                continue
            _, key, value = parts
            store.set(key, value)
        elif cmd == "GET":
            if len(parts) != 2:
                sys.stdout.buffer.write(b"ERR wrong number of arguments for 'GET'\n")
                sys.stdout.flush()
                continue
            _, key = parts
            value = store.get(key)
            if value is not None:
                sys.stdout.buffer.write((value + "\n").encode("utf-8", errors="replace"))
            else:
                sys.stdout.buffer.write(b"NULL\n")
            sys.stdout.flush()
        else:
            sys.stdout.buffer.write(b"ERR unknown command\n")
            sys.stdout.flush()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

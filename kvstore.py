#!/usr/bin/env python3
# KV Store Project 1 - Simple Append-Only Key-Value Store
# Author: Badrinath | EUID: 11820168
# Requirements satisfied:
# - CLI over STDIN/STDOUT: SET <key> <value>, GET <key>, EXIT
# - Append-only persistence to data.db with fsync per write
# - Replay log on startup to rebuild in-memory index
# - No dict/map: uses a list and linear scans; last-write-wins enforced
# - Values may contain spaces (parsed with split(maxsplit=2))

import sys
import os
from typing import Optional

DATA_FILE = "data.db"


class KeyValueStore:
    def __init__(self):
        # In-memory index: list[tuple[str, str]]
        # We keep only the latest value per key by overwriting in place.
        self.index = []
        self.load_data()

    def load_data(self):
        """Replay append-only log into memory."""
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
                # ignore malformed/non-SET lines silently

    def _set_in_memory(self, key: str, value: str):
        """Overwrite if key exists; else append. O(n), acceptable for Part 1."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str):
        """Append to log (durable) then update in-memory view."""
        # Defensive UTF-8 sanitization to keep file valid text.
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")

        with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
            f.write(f"SET {safe_key} {safe_value}\n")
            f.flush()
            os.fsync(f.fileno())

        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """Return latest value or None."""
        # Linear search; because we overwrite on SET, the first match is the latest.
        for k, v in self.index:
            if k == key:
                return v
        return None


def main():
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
                # Write exact value + newline
                sys.stdout.buffer.write((value + "\n").encode("utf-8"))
            else:
                sys.stdout.buffer.write(b"NULL\n")
            sys.stdout.flush()

        else:
            sys.stdout.buffer.write(b"ERR unknown command\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()

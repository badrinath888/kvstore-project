#!/usr/bin/env python3
"""
KV Store Project 1 — Simple Append-Only Key-Value Store
Course: CSCE 5350
Author: Badrinath | EUID: 11820168

Implements a basic key-value store with:
- Commands: SET <key> <value>, GET <key>, EXIT
- Persistent append-only storage (data.db)
- In-memory index rebuilt at startup
- Last-write-wins semantics
"""

import os
import sys
from typing import List, Tuple, Optional

# File used for persistent storage
DATA_FILE = "data.db"


class KVError(Exception):
    """Custom exception for invalid CLI usage (wrong args, unknown command)."""
    pass


class KeyValueStore:
    """
    Append-only persistent key-value store with a simple in-memory index.

    Log Format:
        SET <key> <value>
    """

    def __init__(self) -> None:
        """Initialize the store and rebuild index from log file if it exists."""
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """
        Replay the append-only log into memory.

        Rebuilds the in-memory index by reading each "SET" line
        from data.db. Last write for each key wins.
        """
        if not os.path.exists(DATA_FILE):
            return

        try:
            with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as file:
                for raw_line in file:
                    line = raw_line.strip()
                    if not line:
                        continue
                    parts = line.split(maxsplit=2)
                    if len(parts) == 3 and parts[0].upper() == "SET":
                        _, key, value = parts
                        self._set_in_memory(key, value)
        except OSError as e:
            sys.stderr.write(f"ERR: failed to load {DATA_FILE} — {e.strerror}\n")

    def _set_in_memory(self, key: str, value: str) -> None:
        """Update in-memory index, replacing key if it already exists."""
        for i, (existing_key, _) in enumerate(self.index):
            if existing_key == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair.

        Appends a log entry to data.db and updates the in-memory index.
        Ensures durability with flush + fsync.
        """
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")

        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as file:
                file.write(f"SET {safe_key} {safe_value}\n")
                file.flush()
                os.fsync(file.fileno())
        except OSError as e:
            sys.stderr.write(f"ERR: failed to write {DATA_FILE} — {e.strerror}\n")
            return

        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the latest value for a key.

        Returns:
            str: value if found
            None: if key does not exist
        """
        for existing_key, value in self.index:
            if existing_key == key:
                return value
        return None


# ---- CLI helpers -------------------------------------------------------------

def _write_line(text: str) -> None:
    """Write a line safely to stdout as UTF-8."""
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """Print an error message prefixed with ERR: """
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a single CLI line into command and arguments.

    Returns:
        (cmd, args) where cmd is uppercase string, args is list[str]
    """
    parts = line.strip().split(maxsplit=2)
    if not parts:
        return "", []
    cmd = parts[0].upper()
    args: List[str] = []
    if len(parts) > 1:
        args.append(parts[1])
    if len(parts) > 2:
        args.append(parts[2])
    return cmd, args


def main() -> None:
    """Run the CLI loop for interactive use or automated testing."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    store = KeyValueStore()

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        cmd, args = _parse_command(line)

        try:
            if cmd == "EXIT":
                break
            elif cmd == "SET":
                if len(args) != 2:
                    raise KVError("expected: SET <key> <value>")
                key, value = args
                store.set(key, value)
            elif cmd == "GET":
                if len(args) != 1:
                    raise KVError("expected: GET <key>")
                key = args[0]
                value = store.get(key)
                _write_line(value if value is not None else "NULL")
            elif cmd == "":
                continue
            else:
                raise KVError("unknown command (use SET/GET/EXIT)")
        except KVError as e:
            _err(str(e))
        except OSError as e:
            _err(f"file operation failed — {e.strerror}")
        except Exception as e:
            _err(f"unexpected {type(e).__name__} — {str(e)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass





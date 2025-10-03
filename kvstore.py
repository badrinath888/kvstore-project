#!/usr/bin/env python3
# KV Store Project 1 — Simple Append-Only Key-Value Store
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import sys
from typing import List, Tuple, Optional

DATA_FILE = "data.db"

class KVError(Exception):
    """Custom exception for invalid CLI usage (wrong args, unknown command)."""
    pass


class KeyValueStore:
    """
    Append-only persistent key-value store with a simple in-memory index.

    • Each command is stored in `data.db` as: "SET <key> <value>"
    • On startup, we replay the log file to rebuild state in memory
    • Last-write-wins: the most recent SET for a key is the stored value
    """

    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []  # Simple list instead of dict (per project rules)
        self.load_data()

    def load_data(self) -> None:
        """Replay the append-only log into memory; skip malformed lines."""
        if not os.path.exists(DATA_FILE):
            return
        try:
            with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
                for raw in f:
                    line = raw.strip()
                    if not line:
                        continue
                    parts = line.split(maxsplit=2)
                    # Only accept valid SET lines with key + value
                    if len(parts) == 3 and parts[0].upper() == "SET":
                        _, key, value = parts
                        self._set_in_memory(key, value)
        except OSError as e:
            sys.stderr.write(f"ERR: Failed to load {DATA_FILE} — {e.strerror}\n")

    def _set_in_memory(self, key: str, value: str) -> None:
        """Update in-memory index with last-write-wins semantics."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """Append command to log and update in-memory index."""
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {safe_key} {safe_value}\n")
                f.flush()
                os.fsync(f.fileno())  # Ensure durability
        except OSError as e:
            sys.stderr.write(f"ERR: Failed to write {DATA_FILE} — {e.strerror}\n")
            return
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """Return latest value for key, or None if not found."""
        for k, v in self.index:
            if k == key:
                return v
        return None


# ---- CLI helpers -------------------------------------------------------------

def _write_line(text: str) -> None:
    """Write text to stdout with UTF-8 encoding."""
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()

def _err(msg: str) -> None:
    """Write standardized error message to stdout."""
    _write_line(f"ERR: {msg}")

def _parse_command(line: str) -> Tuple[str, List[str]]:
    """Parse raw CLI input into command + arguments."""
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
    """Main loop: process commands until EXIT."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        # Not all Python environments support reconfigure
        pass

    store = KeyValueStore()

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue  # Ignore blank lines
        cmd, args = _parse_command(line)
        try:
            if cmd == "EXIT":
                break
            elif cmd == "SET":
                if len(args) < 2:
                    raise KVError(f"SET requires <key> and <value>, got {len(args)} argument(s).")
                elif len(args) > 2:
                    raise KVError("SET only accepts 2 arguments: <key> <value>.")
                key, value = args
                store.set(key, value)
            elif cmd == "GET":
                if len(args) == 0:
                    raise KVError("GET requires exactly 1 argument: <key>.")
                elif len(args) > 1:
                    raise KVError("GET only accepts 1 argument: <key>.")
                key = args[0]
                value = store.get(key)
                _write_line(value if value is not None else "NULL")
            elif cmd == "":
                continue  # skip empty input
            else:
                raise KVError(f"Unknown command '{cmd}'. Valid commands: SET, GET, EXIT.")
        except KVError as e:
            _err(str(e))
        except OSError as e:
            _err(f"File operation failed — {e.strerror}")
        except Exception as e:
            _err(f"Unexpected {type(e).__name__} — {str(e)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass






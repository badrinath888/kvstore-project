#!/usr/bin/env python3
# KV Store Project 1 — Simple Append-Only Key-Value Store
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import sys
from typing import List, Tuple, Optional

DATA_FILE = "data.db"


class KVError(Exception):
    """Exception for invalid CLI usage (wrong args, unknown command)."""
    pass


class KeyValueStore:
    """
    Append-only persistent key-value store with a simple in-memory index.

    Design
    ------
    • Data is logged in `data.db` with lines like: SET <key> <value>
    • On startup, log is replayed into memory to rebuild the index.
    • Last-write-wins: the latest SET for a key replaces older values.
    • Uses a list of tuples [(key, value)] instead of dict/map (per assignment).
    """

    def __init__(self) -> None:
        """Initialize store and rebuild index by replaying the log."""
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """
        Replay the log into memory.

        Reads `data.db` line by line. Valid entries update the in-memory index.
        Malformed lines or I/O errors are skipped safely.
        """
        if not os.path.exists(DATA_FILE):
            return
        try:
            with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
                for raw in f:
                    parts = raw.strip().split(maxsplit=2)
                    if len(parts) == 3 and parts[0].upper() == "SET":
                        _, key, value = parts
                        self._set_in_memory(key, value)
        except OSError:
            # Fail gracefully if file is unreadable
            return

    def _set_in_memory(self, key: str, value: str) -> None:
        """Insert or overwrite (key, value) in the index."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """Persist a SET and update index with fsync for durability."""
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
            f.write(f"SET {safe_key} {safe_value}\n")
            f.flush()
            os.fsync(f.fileno())
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """Return latest value for key, or None if not found."""
        for k, v in self.index:
            if k == key:
                return v
        return None


# ───── CLI helpers ────────────────────────────────────────────────────────────

def _write_line(text: str) -> None:
    """Write a UTF-8 line to STDOUT and flush immediately."""
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """Emit a normalized error message."""
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """Parse input into (command, args)."""
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
    """Main CLI loop: supports SET, GET, EXIT."""
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

        cmd, args = _parse_command(line)
        try:
            if cmd == "EXIT":
                break
            elif cmd == "SET":
                if len(args) != 2:
                    raise KVError("expected: SET <key> <value>")
                store.set(args[0], args[1])
            elif cmd == "GET":
                if len(args) != 1:
                    raise KVError("expected: GET <key>")
                value = store.get(args[0])
                _write_line(value if value is not None else "NULL")
            elif cmd == "":
                continue
            else:
                raise KVError("unknown command (use SET/GET/EXIT)")
        except KVError as e:
            _err(str(e))
        except Exception:
            _err("internal error")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass




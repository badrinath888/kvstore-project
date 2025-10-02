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

    Design
    ------
    • Data is logged in `data.db` as lines: "SET <key> <value>".
    • On startup, the log is replayed into an in-memory list of (key, value) pairs.
    • **Last-write-wins**: the latest SET for a key overwrites older values.
    • No built-in dictionaries/maps are used (per assignment restriction).
    """

    def __init__(self) -> None:
        """Initialize the store and rebuild the in-memory index by log replay."""
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """
        Replay the append-only log into memory.

        Iterates `data.db` line by line. Only lines exactly matching
        "SET <key> <value>" (space-delimited, 3 tokens, case-insensitive SET)
        are applied. Malformed/truncated lines are skipped for robustness.
        """
        if not os.path.exists(DATA_FILE):
            return

        try:
            with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as file:
                for line in file:
                    clean = line.strip()
                    if not clean:
                        continue
                    parts = clean.split(maxsplit=2)
                    if len(parts) == 3 and parts[0].upper() == "SET":
                        _, key, value = parts
                        self._set_in_memory(key, value)
        except OSError as e:
            # Don’t crash; surface a clear error for quality review.
            sys.stderr.write(f"ERR: failed to load {DATA_FILE} — {e.strerror}\n")

    def _set_in_memory(self, key: str, value: str) -> None:
        """
        Insert or overwrite a (key, value) pair in the in-memory linear index.

        Parameters
        ----------
        key : str
            Key to store (single token).
        value : str
            Associated value (arbitrary UTF-8 text).
        """
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Persist and index a SET operation with flush + fsync for durability.

        Steps
        -----
        1) Append "SET <key> <value>\\n" to `data.db`
        2) Flush and fsync file descriptor
        3) Update in-memory index
        """
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {safe_key} {safe_value}\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            sys.stderr.write(f"ERR: failed to write {DATA_FILE} — {e.strerror}\n")
            return
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the most recent value for a key.

        Returns
        -------
        Optional[str] : the latest value if present; else None
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


# ───── CLI helpers ────────────────────────────────────────────────────────────

def _write_line(text: str) -> None:
    """Write a single line to STDOUT in UTF-8 and flush."""
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """Write a normalized error line to STDOUT."""
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a raw input line into (command, args).

    Split into at most 3 parts so SET values can include spaces.
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
    """
    Main CLI loop.

    Commands
    --------
    • SET <key> <value>
    • GET <key>
    • EXIT
    """
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

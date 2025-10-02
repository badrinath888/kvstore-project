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
    • Data is logged in `data.db` with lines formatted as "SET <key> <value>".
    • On startup, the log is replayed into an in-memory list of (key, value) pairs.
    • The index uses **last-write-wins** semantics: the latest SET overwrites older values.
    • No built-in dictionaries or maps are used (per assignment restriction).
    """

    def __init__(self) -> None:
        """Initialize the store and rebuild in-memory index from log replay."""
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """
        Replay the append-only log into memory.

        Reads each line from `data.db`. Valid lines of the form "SET <key> <value>"
        update the in-memory index so that the newest value for a key replaces the old one.

        Malformed or truncated lines are ignored for robustness.
        """
        if not os.path.exists(DATA_FILE):
            return

        try:
            with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as file:
                for line in file:
                    clean_line = line.strip()
                    if not clean_line:
                        continue
                    parts = clean_line.split(maxsplit=2)
                    if len(parts) == 3 and parts[0].upper() == "SET":
                        _, key, value = parts
                        self._set_in_memory(key, value)
        except OSError as e:
            sys.stderr.write(f"ERR: failed to load log file — {e}\n")

    def _set_in_memory(self, key: str, value: str) -> None:
        """
        Insert or overwrite `(key, value)` in the in-memory list.

        Parameters
        ----------
        key : str
            The key to set.
        value : str
            The value to associate with the key.
        """
        for i, (existing_key, _) in enumerate(self.index):
            if existing_key == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Persist and index a SET operation.

        Steps
        -----
        1. Append a line "SET <key> <value>" to `data.db`.
        2. Flush and fsync for durability.
        3. Update the in-memory index.
        """
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as file:
                file.write(f"SET {safe_key} {safe_value}\n")
                file.flush()
                os.fsync(file.fileno())
        except OSError as e:
            sys.stderr.write(f"ERR: failed to write log file — {e}\n")
            return
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the most recent value for a key.

        Returns the latest value if the key exists, otherwise None.
        """
        for stored_key, stored_value in self.index:
            if stored_key == key:
                return stored_value
        return None


# ───── CLI helpers ────────────────────────────────────────────────────────────

def _write_line(text: str) -> None:
    """Write a single line of text to STDOUT in UTF-8 and flush."""
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """Write a normalized error message to STDOUT."""
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a raw input line into (command, args).

    Splits into at most 3 parts so that SET values can contain spaces.
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
    Main CLI loop for the key-value store.

    Supports:
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

    for line in sys.stdin:
        clean_line = line.strip()
        if not clean_line:
            continue

        cmd, args = _parse_command(clean_line)
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
            _err(f"ERR: file operation failed — {e.strerror}")
        except Exception as e:
            _err(f"ERR: unexpected internal error — {type(e).__name__}")



if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

﻿#!/usr/bin/env python3
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
    Append-only persistent key-value store with an in-memory index.

    The store appends all `SET` operations to a log file (data.db).
    On startup, it replays this log to rebuild state in memory.
    Last-write-wins semantics are enforced.
    """

    def __init__(self) -> None:
        """
        Initialize a new KeyValueStore instance.

        Replays the log file if present to rebuild the in-memory index.
        """
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """
        Replay the append-only log (data.db) into memory.

        Skips malformed lines gracefully. If the log file does not exist,
        nothing is loaded.
        """
        if not os.path.exists(DATA_FILE):
            return
        try:
            with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
                for raw in f:
                    line = raw.strip()
                    if not line:
                        continue
                    parts = line.split(maxsplit=2)
                    if len(parts) == 3 and parts[0].upper() == "SET":
                        _, key, value = parts
                        self._set_in_memory(key, value)
        except OSError as e:
            sys.stderr.write(f"ERR: failed to load {DATA_FILE} — {e.strerror}\n")

    def _set_in_memory(self, key: str, value: str) -> None:
        """
        Update or insert the key-value pair in the in-memory index.

        Args:
            key (str): The key to store.
            value (str): The value to associate with the key.
        """
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair to the log and update in-memory index.

        Args:
            key (str): The key to set.
            value (str): The value to associate with the key.

        Raises:
            OSError: If writing to the file fails.
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
        Retrieve the value for a given key.

        Args:
            key (str): The key to look up.

        Returns:
            Optional[str]: The associated value if present, otherwise None.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


# ---- CLI helpers -------------------------------------------------------------

def _write_line(text: str) -> None:
    """
    Write a line to STDOUT with UTF-8 encoding.

    Args:
        text (str): The text to output.
    """
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """
    Write a standardized error message to STDOUT.

    Args:
        msg (str): The error message.
    """
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a user input line into a command and its arguments.

    Args:
        line (str): The raw command line input.

    Returns:
        Tuple[str, List[str]]: Command name (uppercased), and list of args.
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
    Main entry point for the CLI.

    Reads commands from STDIN and executes them against the key-value store.
    Commands supported:
        - SET <key> <value>
        - GET <key>
        - EXIT
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
                    raise KVError("SET requires exactly 2 arguments: SET <key> <value>")
                key, value = args
                store.set(key, value)
            elif cmd == "GET":
                if len(args) != 1:
                    raise KVError("GET requires exactly 1 argument: GET <key>")
                key = args[0]
                value = store.get(key)
                _write_line(value if value is not None else "NULL")
            elif cmd == "":
                continue
            else:
                raise KVError(f"unknown command '{cmd}' (supported: SET, GET, EXIT)")
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



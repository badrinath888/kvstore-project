#!/usr/bin/env python3
"""
CSCE 5350 — Project 1: Simple Append-Only Key-Value Store
Author: Badrinath | EUID: 11820168

This module implements a persistent key-value store with a simple command-line
interface. Data is stored in append-only format in `data.db`, ensuring durability
across program restarts. Keys are mapped to values using an in-memory index
(last write wins semantics). Built-in Python dictionaries are intentionally
not used for indexing (per project requirements).
"""

import os
import sys
from typing import List, Tuple, Optional

# The persistent log file name for the key-value store
DATA_FILE = "data.db"


class KVError(Exception):
    """
    Custom exception type for invalid CLI usage.

    Raised when:
        • The user provides an incorrect number of arguments.
        • The user enters an unknown command.
        • A file operation fails unexpectedly.
    """
    pass


class KeyValueStore:
    """
    Append-only persistent key-value store with in-memory index.

    Design:
        • Every SET command is appended to `data.db`.
        • On startup, the log file is replayed to rebuild the index.
        • The in-memory index is a list of (key, value) tuples.
        • "Last write wins" semantics ensure the most recent value is returned.
    """

    def __init__(self) -> None:
        """Initialize the store and load any previously persisted data."""
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """
        Rebuild in-memory index by replaying the append-only log.

        Malformed lines are skipped silently. Only lines in the format
        'SET <key> <value>' are recognized and applied.
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
        except OSError as err:
            sys.stderr.write(f"ERR: failed to load {DATA_FILE} — {err.strerror}\n")

    def _set_in_memory(self, key: str, value: str) -> None:
        """
        Update the in-memory index with a new key-value pair.

        If the key already exists, its value is replaced (last write wins).
        """
        for i, (existing_key, _) in enumerate(self.index):
            if existing_key == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair by appending it to the log, then update index.

        Args:
            key (str): The key to store.
            value (str): The value to associate with the key.
        """
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as file:
                file.write(f"SET {safe_key} {safe_value}\n")
                file.flush()
                os.fsync(file.fileno())  # Ensure durability on disk
        except OSError as err:
            sys.stderr.write(f"ERR: failed to write {DATA_FILE} — {err.strerror}\n")
            return
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value for a given key.

        Args:
            key (str): The key to look up.

        Returns:
            str: The associated value if found.
            None: If the key does not exist.
        """
        for stored_key, stored_value in self.index:
            if stored_key == key:
                return stored_value
        return None


# ---- CLI helper functions ---------------------------------------------------

def _write_line(text: str) -> None:
    """
    Write a line of output to STDOUT using UTF-8 encoding.

    Args:
        text (str): The message to write.
    """
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(message: str) -> None:
    """
    Write an error message prefixed with 'ERR:'.

    Args:
        message (str): The error details to display.
    """
    _write_line(f"ERR: {message}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a raw input line into a command and argument list.

    Args:
        line (str): The user input string.

    Returns:
        Tuple[str, List[str]]:
            Command name (uppercase), followed by arguments list.
    """
    parts = line.strip().split(maxsplit=2)
    if not parts:
        return "", []
    command = parts[0].upper()
    args: List[str] = []
    if len(parts) > 1:
        args.append(parts[1])
    if len(parts) > 2:
        args.append(parts[2])
    return command, args


def main() -> None:
    """
    Entry point for CLI.

    Supported commands:
        • SET <key> <value>
        • GET <key>
        • EXIT

    Invalid commands or argument counts produce error messages prefixed with ERR.
    """
    # Configure UTF-8 for stdin/stdout for portability
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass  # Ignore if running on platforms without reconfigure()

    store = KeyValueStore()

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        command, arguments = _parse_command(line)
        try:
            if command == "EXIT":
                break
            elif command == "SET":
                if len(arguments) != 2:
                    raise KVError("expected: SET <key> <value>")
                key, value = arguments
                store.set(key, value)
            elif command == "GET":
                if len(arguments) != 1:
                    raise KVError("expected: GET <key>")
                key = arguments[0]
                value = store.get(key)
                _write_line(value if value is not None else "NULL")
            elif command == "":
                continue
            else:
                raise KVError("unknown command (use SET/GET/EXIT)")
        except KVError as err:
            _err(str(err))
        except OSError as err:
            _err(f"file operation failed — {err.strerror}")
        except Exception as err:
            _err(f"unexpected {type(err).__name__} — {str(err)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass



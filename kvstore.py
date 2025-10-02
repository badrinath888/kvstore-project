#!/usr/bin/env python3
# KV Store Project 1 — Simple Append-Only Key-Value Store
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import sys
from typing import List, Tuple, Optional

DATA_FILE = "data.db"


class KVError(Exception):
    """Custom exception for invalid CLI usage (wrong arguments or unknown command)."""
    pass


class KeyValueStore:
    """A simple append-only persistent key-value store.

    Data is persisted in an append-only file (`data.db`).
    The store maintains an in-memory index that is rebuilt on startup
    by replaying the log file. For each key, the last written value
    is the one that is returned (last-write-wins).
    """

    def __init__(self) -> None:
        """Initialize the store and rebuild the in-memory index from disk."""
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """Rebuild the in-memory index from the append-only log file.

        Reads each line of `data.db` and replays all valid `SET` commands.
        Malformed or incomplete lines are skipped.
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
        except OSError as error:
            sys.stderr.write(f"ERR: failed to load {DATA_FILE} — {error.strerror}\n")

    def _set_in_memory(self, key: str, value: str) -> None:
        """Update the in-memory index for a given key.

        If the key already exists, overwrite its value.
        Otherwise, append the new key-value pair.
        """
        for position, (existing_key, _) in enumerate(self.index):
            if existing_key == key:
                self.index[position] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """Persist a key-value pair to the log file and update in-memory index.

        Args:
            key (str): The key name.
            value (str): The value to be associated with the key.
        """
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as file:
                file.write(f"SET {safe_key} {safe_value}\n")
                file.flush()
                os.fsync(file.fileno())  # ensure durability
        except OSError as error:
            sys.stderr.write(f"ERR: failed to write {DATA_FILE} — {error.strerror}\n")
            return
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """Retrieve the most recent value associated with a key.

        Args:
            key (str): The key to look up.

        Returns:
            Optional[str]: The value if found, otherwise None.
        """
        for existing_key, value in self.index:
            if existing_key == key:
                return value
        return None


# ---- CLI helpers -------------------------------------------------------------


def _write_line(text: str) -> None:
    """Write a single line of text to STDOUT in UTF-8 encoding."""
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(message: str) -> None:
    """Write an error message with ERR prefix to STDOUT."""
    _write_line(f"ERR: {message}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """Parse a raw input line into (command, arguments)."""
    parts = line.strip().split(maxsplit=2)
    if not parts:
        return "", []
    command = parts[0].upper()
    arguments: List[str] = []
    if len(parts) > 1:
        arguments.append(parts[1])
    if len(parts) > 2:
        arguments.append(parts[2])
    return command, arguments


def main() -> None:
    """Main interactive loop for the KeyValueStore CLI."""
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
        command, arguments = _parse_command(line)
        try:
            if command == "EXIT":
                break
            elif command == "SET":
                if len(arguments) != 2:
                    raise KVError("expected: SET <key> <value>")
                key_name, key_value = arguments
                store.set(key_name, key_value)
            elif command == "GET":
                if len(arguments) != 1:
                    raise KVError("expected: GET <key>")
                key_name = arguments[0]
                value = store.get(key_name)
                _write_line(value if value is not None else "NULL")
            elif command == "":
                continue
            else:
                raise KVError("unknown command (use SET/GET/EXIT)")
        except KVError as error:
            _err(str(error))
        except OSError as error:
            _err(f"file operation failed — {error.strerror}")
        except Exception as error:
            _err(f"unexpected {type(error).__name__} — {str(error)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass


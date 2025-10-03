#!/usr/bin/env python3
"""
CSCE 5350 — Project 1: Simple Append-Only Key-Value Store
Author: Badrinath
EUID: 11820168

This file implements a persistent key-value store with:
- SET <key> <value>
- GET <key>
- EXIT

Data is stored in an append-only log file (`data.db`).
On restart, the log is replayed to rebuild the in-memory index.
"""

import os
import sys
from typing import List, Tuple


class KVStore:
    """
    A simple persistent key-value store.
    Uses an append-only log to ensure durability and last-write-wins semantics.
    """

    def __init__(self, filename: str = "data.db") -> None:
        """
        Initialize the store and replay existing log file into memory.
        """
        self.filename = filename
        self.index: List[Tuple[str, str]] = []  # list of (key, value) pairs

        # Rebuild index from existing log file
        if os.path.exists(self.filename):
            with open(self.filename, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        command, key, value = line.strip().split(" ", 2)
                        if command == "SET":
                            self._update_index(key, value)
                    except ValueError:
                        # Ignore malformed lines instead of crashing
                        continue

    def _update_index(self, key: str, value: str) -> None:
        """
        Internal helper: update key in index (last-write-wins).
        """
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Store a key-value pair persistently and in memory.
        """
        if key is None or not isinstance(key, str):
            raise ValueError("Key must be a non-null string")

        # Append to log
        with open(self.filename, "a", encoding="utf-8") as f:
            f.write(f"SET {key} {value}\n")
            f.flush()
            os.fsync(f.fileno())

        # Update in-memory index
        self._update_index(key, value)

    def get(self, key: str) -> str:
        """
        Retrieve the value for a given key.
        Returns 'NULL' if the key does not exist.
        """
        for k, v in self.index:
            if k == key:
                return v
        return "NULL"


def main() -> None:
    """
    Command-Line Interface (CLI) loop.
    Accepts SET, GET, EXIT commands from the user.
    """
    store = KVStore()

    print("Welcome to the CSCE 5350 KVStore (Project 1). Type EXIT to quit.")
    while True:
        try:
            command_line = input("> ").strip()
            if not command_line:
                continue

            parts = command_line.split(" ", 2)
            command = parts[0].upper()

            if command == "SET":
                if len(parts) < 3:
                    print("ERROR: SET requires both <key> and <value>")
                else:
                    key, value = parts[1], parts[2]
                    store.set(key, value)
                    print("OK")

            elif command == "GET":
                if len(parts) < 2:
                    print("ERROR: GET requires <key>")
                else:
                    key = parts[1]
                    print(store.get(key))

            elif command == "EXIT":
                print("Exiting KVStore. Goodbye!")
                break

            else:
                print(f"ERROR: Unknown command '{command}'. Valid commands: SET, GET, EXIT.")

        except (KeyboardInterrupt, EOFError):
            # Handle Ctrl+C / Ctrl+D gracefully
            print("\nExiting KVStore. Goodbye!")
            break
        except Exception as e:
            # Catch-all for unexpected errors
            print(f"ERROR: {str(e)}", file=sys.stderr)


if __name__ == "__main__":
    main()


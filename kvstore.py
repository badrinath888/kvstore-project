#!/usr/bin/env python3
"""
KV Store Project 1 — Simple Append-Only Key-Value Store
Course: CSCE 5350
Author: Badrinath | EUID: 11820168

Implements a persistent key-value store supporting:
- SET <key> <value>
- GET <key>
- EXIT

Persistence:
- Append-only file `data.db`
- Replay log on startup to rebuild in-memory index
- Last-write-wins semantics
"""

import os
import sys
import logging
from typing import Optional, List, Tuple

# Configure logging for better debugging/maintainability
logging.basicConfig(
    filename="kvstore.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


class KVStore:
    """
    A simple key-value store with append-only persistence.
    """

    def __init__(self, filename: str = "data.db"):
        self.filename = filename
        self.index: List[Tuple[str, str]] = []  # simple linear index
        self._load()

    def _load(self) -> None:
        """Replay the log file to rebuild in-memory state."""
        if not os.path.exists(self.filename):
            return
        try:
            with open(self.filename, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split(" ", 2)
                    if len(parts) == 3 and parts[0] == "SET":
                        _, key, value = parts
                        self._set_in_memory(key, value)
        except Exception as e:
            logging.error(f"Error loading data file: {e}")

    def _set_in_memory(self, key: str, value: str) -> None:
        """Update the in-memory index."""
        self.index = [(k, v) for (k, v) in self.index if k != key]
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Store a key-value pair.
        Persists immediately to disk.
        """
        try:
            with open(self.filename, "a", encoding="utf-8") as f:
                f.write(f"SET {key} {value}\n")
                f.flush()
                os.fsync(f.fileno())
            self._set_in_memory(key, value)
            logging.info(f"SET key={key} value={value}")
        except Exception as e:
            logging.error(f"Failed to set key {key}: {e}")
            raise

    def get(self, key: str) -> Optional[str]:
        """Retrieve the value for a given key."""
        for k, v in reversed(self.index):
            if k == key:
                return v
        return None


def main() -> None:
    """
    Command-line interface.
    Supports:
      SET <key> <value>
      GET <key>
      EXIT
    """
    store = KVStore()
    print("KVStore ready. Type commands: SET <key> <value>, GET <key>, EXIT")

    while True:
        try:
            user_input = input("> ").strip()
            if not user_input:
                continue

            parts = user_input.split(" ", 2)
            cmd = parts[0].upper()

            if cmd == "SET":
                if len(parts) < 3:
                    print("ERROR: SET requires <key> and <value>")
                    logging.warning("Invalid SET command: missing key/value")
                    continue
                _, key, value = parts
                store.set(key, value)
                print("OK")

            elif cmd == "GET":
                if len(parts) != 2:
                    print("ERROR: GET requires exactly one <key>")
                    logging.warning("Invalid GET command format")
                    continue
                _, key = parts
                value = store.get(key)
                if value is not None:
                    print(value)
                else:
                    print("NULL")

            elif cmd == "EXIT":
                print("Exiting KVStore.")
                logging.info("Shutdown requested by user")
                break

            else:
                print(f"ERROR: Unknown command '{cmd}'")
                logging.warning(f"Unknown command: {cmd}")

        except KeyboardInterrupt:
            print("\nExiting KVStore.")
            logging.info("Exited with keyboard interrupt")
            break
        except Exception as e:
            print(f"ERROR: {e}")
            logging.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()




#!/usr/bin/env python3
"""
CSCE 5350 — Project 1: Simple Append-Only Key-Value Store
Author: Badrinath
EUID: 11820168

A persistent key-value store with:
- SET <key> <value>
- GET <key>
- EXIT

Data is stored in an append-only log (data.db). On startup, the store replays
the log to rebuild the in-memory index. Implements last-write-wins semantics.
"""

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, List, Tuple


DATA_FILE = "data.db"
LOG_FILE = "kvstore.log"

# ---------------- Logging Setup ----------------
logger = logging.getLogger("kvstore")
logger.setLevel(logging.DEBUG)

_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3)
_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_handler.setFormatter(_formatter)
logger.addHandler(_handler)

_console = logging.StreamHandler()
_console.setFormatter(_formatter)
logger.addHandler(_console)


# ---------------- Custom Exception ----------------
class KVError(Exception):
    """Custom exception for errors in the key-value store."""
    pass


# ---------------- KV Store ----------------
class KVStore:
    """
    A simple append-only key-value store.

    Attributes:
        data_file (str): Path to the data file.
        index (List[Tuple[str, str]]): In-memory key-value pairs.
    """

    def __init__(self, data_file: str = DATA_FILE) -> None:
        """
        Initialize the store and load existing data.

        Args:
            data_file (str): Path to the append-only log file.
        """
        self.data_file = data_file
        self.index: List[Tuple[str, str]] = []
        logger.info("Starting KVStore with data file=%s", self.data_file)
        self._load()

    def _load(self) -> None:
        """
        Load data from the append-only log file into memory.

        Raises:
            KVError: If the file cannot be read or decoded.
        """
        if not os.path.exists(self.data_file):
            logger.info("Data file %s not found. Starting fresh.", self.data_file)
            return
        try:
            with open(self.data_file, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split(" ", 2)
                    if len(parts) == 3 and parts[0] == "SET":
                        _, key, value = parts
                        self._set_index(key, value)
            logger.info("Loaded %d entries from %s", len(self.index), self.data_file)
        except (OSError, UnicodeDecodeError) as e:
            logger.exception("Failed to load %s", self.data_file)
            raise KVError(f"Error loading {self.data_file}") from e

    def _set_index(self, key: str, value: str) -> None:
        """
        Update or insert a key-value pair in memory.

        Args:
            key (str): The key to insert/update.
            value (str): The associated value.
        """
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair and update memory.

        Args:
            key (str): Key string.
            value (str): Value string.

        Raises:
            KVError: If writing to the file fails.
        """
        try:
            with open(self.data_file, "a", encoding="utf-8") as f:
                f.write(f"SET {key} {value}\n")
                f.flush()
                os.fsync(f.fileno())
            self._set_index(key, value)
            logger.debug("SET key=%s value=%s", key, value)
        except OSError as e:
            logger.exception("Failed to persist key=%s", key)
            raise KVError(f"Unable to set key '{key}' due to I/O error.") from e

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the latest value for a key.

        Args:
            key (str): Key string.

        Returns:
            Optional[str]: Value if found, otherwise None.
        """
        for k, v in reversed(self.index):
            if k == key:
                logger.debug("GET key=%s -> value=%s", key, v)
                return v
        logger.debug("GET key=%s -> NOT FOUND", key)
        return None


# ---------------- CLI ----------------
def _write_out(msg: str) -> None:
    """Print to STDOUT safely."""
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def _write_err(msg: str) -> None:
    """Print error to STDERR safely."""
    sys.stderr.write("ERR: " + msg + "\n")
    sys.stderr.flush()


def main() -> None:
    """
    Command-line interface loop.
    Handles SET, GET, and EXIT commands from STDIN.
    """
    store = KVStore()

    while True:
        try:
            line = sys.stdin.readline()
            if not line:  # EOF
                logger.info("EOF reached, exiting.")
                break

            parts = line.strip().split(" ", 2)
            if not parts or parts[0] == "":
                continue

            cmd = parts[0].upper()

            if cmd == "EXIT":
                logger.info("EXIT command received.")
                break
            elif cmd == "SET":
                if len(parts) != 3:
                    _write_err("Invalid SET syntax. Usage: SET <key> <value>")
                    continue
                key, value = parts[1], parts[2]
                store.set(key, value)
            elif cmd == "GET":
                if len(parts) != 2:
                    _write_err("Invalid GET syntax. Usage: GET <key>")
                    continue
                key = parts[1]
                value = store.get(key)
                _write_out(value if value is not None else "NULL")
            else:
                _write_err(f"Unknown command: {cmd}. Valid commands: SET, GET, EXIT.")
                logger.warning("Unknown command received: %s", cmd)

        except KVError as e:
            _write_err(str(e))
            logger.error("KVError: %s", e)
        except Exception as e:
            _write_err("Unexpected internal error occurred. Please try again.")
            logger.exception("Unhandled exception in CLI loop: %s", e)


if __name__ == "__main__":
    main()








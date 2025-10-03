#!/usr/bin/env python3
"""
CSCE 5350 — Project 1: Simple Append-Only Key-Value Store
Author: Badrinath
EUID: 11820168

This module implements a persistent key-value store with the following features:
- SET <key> <value>
- GET <key>
- EXIT

Data is persisted using an append-only log (`data.db`).
On restart, the store replays the log to rebuild the in-memory index.
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

# rotating log file: 1 MB max, keep 3 backups
_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3)
_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
_handler.setFormatter(_formatter)
logger.addHandler(_handler)

# also log to stderr for immediate feedback
_console = logging.StreamHandler()
_console.setFormatter(_formatter)
logger.addHandler(_console)


# ---------------- Custom Exception ----------------
class KVError(Exception):
    """Custom exception for key-value store errors."""
    pass


# ---------------- KV Store Implementation ----------------
class KVStore:
    """
    A simple append-only key-value store.

    Attributes:
        index (List[Tuple[str, str]]): In-memory list of (key, value) pairs.
    """

    def __init__(self, data_file: str = DATA_FILE) -> None:
        self.data_file = data_file
        self.index: List[Tuple[str, str]] = []
        logger.info("Initializing KVStore with data file=%s", self.data_file)
        self._load()

    def _load(self) -> None:
        """Replay the append-only log into memory."""
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
            logger.exception("Error loading data file %s", self.data_file)
            raise KVError(f"Failed to load {self.data_file}") from e

    def _set_index(self, key: str, value: str) -> None:
        """Helper to insert or overwrite a key in the in-memory index."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Store a key-value pair.
        Writes to disk immediately (append-only).
        """
        try:
            with open(self.data_file, "a", encoding="utf-8") as f:
                f.write(f"SET {key} {value}\n")
                f.flush()
                os.fsync(f.fileno())
            self._set_index(key, value)
            logger.debug("SET key=%s value=%s", key, value)
        except OSError as e:
            logger.exception("I/O error on SET key=%s", key)
            raise KVError(f"Failed to set key {key}") from e

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value for a key.
        Returns None if the key does not exist.
        """
        for k, v in reversed(self.index):  # last write wins
            if k == key:
                logger.debug("GET key=%s -> value=%s", key, v)
                return v
        logger.debug("GET key=%s -> NOT FOUND", key)
        return None


# ---------------- CLI Implementation ----------------
def _write_out(msg: str) -> None:
    """Safe print to STDOUT."""
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def _write_err(msg: str) -> None:
    """Safe print to STDERR."""
    sys.stderr.write("ERR: " + msg + "\n")
    sys.stderr.flush()


def main() -> None:
    """Command-line loop for the KV store."""
    store = KVStore()

    while True:
        try:
            line = sys.stdin.readline()
            if not line:  # EOF
                logger.info("Received EOF, exiting gracefully.")
                break

            parts = line.strip().split(" ", 2)
            if not parts or parts[0] == "":
                continue

            cmd = parts[0].upper()

            if cmd == "EXIT":
                logger.info("EXIT command received. Shutting down.")
                break
            elif cmd == "SET":
                if len(parts) != 3:
                    _write_err("Usage: SET <key> <value>")
                    logger.warning("Invalid SET command syntax: %s", line.strip())
                    continue
                key, value = parts[1], parts[2]
                store.set(key, value)
            elif cmd == "GET":
                if len(parts) != 2:
                    _write_err("Usage: GET <key>")
                    logger.warning("Invalid GET command syntax: %s", line.strip())
                    continue
                key = parts[1]
                value = store.get(key)
                if value is not None:
                    _write_out(value)
                else:
                    _write_out("NULL")
            else:
                _write_err(f"Unknown command: {cmd}")
                logger.warning("Unknown command: %s", cmd)

        except KVError as e:
            _write_err(str(e))
            logger.error("KVError: %s", e)
        except Exception as e:
            _write_err("Unexpected internal error")
            logger.exception("Unhandled exception in CLI loop: %s", e)


if __name__ == "__main__":
    main()







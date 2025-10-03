#!/usr/bin/env python3
"""
KV Store Project 1 — append-only key-value store with improved error handling & logging
Course: CSCE 5350
Author: Badrinath | EUID: 11820168
"""

import os
import sys
import logging
from typing import List, Tuple, Optional

# Configure logger
logger = logging.getLogger(__name__)
handler = logging.FileHandler("kvstore.log")
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)  # you can set DEBUG during development

DATA_FILE = "data.db"


class KVError(Exception):
    """Raised for invalid commands or user errors."""
    pass


class KeyValueStore:
    """Persistent append-only key-value store with in-memory index."""

    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []
        self._load()

    def _load(self) -> None:
        """Replay the data file to rebuild in-memory state."""
        if not os.path.exists(DATA_FILE):
            logger.debug(f"{DATA_FILE} does not exist; skipping load")
            return
        try:
            with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
                for raw in f:
                    line = raw.strip()
                    if not line:
                        continue
                    parts = line.split(maxsplit=2)
                    if len(parts) == 3 and parts[0].upper() == "SET":
                        _, key, val = parts
                        self._set_in_memory(key, val)
        except OSError as e:
            logger.exception("Failed to load data file")
            # if load fails, we continue with empty index (but log it)

    def _set_in_memory(self, key: str, value: str) -> None:
        """Update in-memory mapping (last-write-wins)."""
        # Remove old entry if present
        self.index = [(k, v) for (k, v) in self.index if k != key]
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """Persist a SET command to disk, then update in-memory index."""
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {key} {value}\n")
                f.flush()
                os.fsync(f.fileno())
            self._set_in_memory(key, value)
            logger.info("SET key=%s value=%s", key, value)
        except OSError as e:
            logger.exception("I/O error on SET")
            # wrap or raise a KVError so CLI can catch
            raise KVError(f"Failed to write key '{key}'") from e

    def get(self, key: str) -> Optional[str]:
        """Return the current value for the key, or None if missing."""
        # Search in reverse so latest writes have priority
        for k, v in reversed(self.index):
            if k == key:
                return v
        return None


def _write_line(s: str) -> None:
    """Helper to write a line to stdout safely."""
    try:
        sys.stdout.write(s + "\n")
        sys.stdout.flush()
    except Exception:
        logger.exception("Output write failed")


def _write_err(msg: str) -> None:
    _write_line("ERR: " + msg)


def _parse_cmd(line: str) -> Tuple[str, List[str]]:
    parts = line.strip().split(maxsplit=2)
    if not parts:
        return "", []
    cmd = parts[0].upper()
    args = parts[1:] if len(parts) > 1 else []
    return cmd, args


def main() -> None:
    logger.info("KVStore starting")
    store = KeyValueStore()

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        cmd, args = _parse_cmd(line)
        try:
            if cmd == "EXIT":
                logger.info("EXIT received, shutting down")
                break
            elif cmd == "SET":
                if len(args) != 2:
                    raise KVError("SET requires exactly two arguments: key and value")
                key, value = args
                store.set(key, value)
            elif cmd == "GET":
                if len(args) != 1:
                    raise KVError("GET requires exactly one argument: key")
                key = args[0]
                val = store.get(key)
                _write_line(val if val is not None else "NULL")
            elif cmd == "":
                # blank input, ignore
                continue
            else:
                raise KVError(f"Unknown command '{cmd}'")
        except KVError as e:
            _write_err(str(e))
            logger.warning("User-level error: %s", e)
        except Exception as e:
            # unexpected failure
            logger.exception("Unhandled exception in main loop")
            _write_err("unexpected internal error")

    logger.info("KVStore exiting")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user via keyboard")
        # Clean exit
        pass





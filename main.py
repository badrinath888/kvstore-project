#!/usr/bin/env python3
"""
Project 1: Simple Append-Only Key-Value Store.

CLI commands over STDIN / STDOUT:
  - SET <key> <value>
  - GET <key>
  - EXIT

Design:
  - Append-only file `data.db`
  - Replay file on startup to rebuild an in-memory index
  - In-memory index is a list of (key, value) pairs (no dict), last-write-wins
  - fsync after each SET for durability

This file intentionally avoids Python dicts/maps in the index
to satisfy the project’s constraint for Part 1.
"""

from __future__ import annotations

import os
import sys
import logging
from typing import List, Tuple, Optional


# ---- Constants ----------------------------------------------------------------

DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


# ---- Exceptions ---------------------------------------------------------------

class KVError(Exception):
    """Raised for user/CLI errors (unknown command, wrong arity, etc.)."""
    pass


# ---- Logger setup -------------------------------------------------------------

def _setup_logging(log_path: str = LOG_FILE) -> None:
    """
    Configure module-level logging.

    Args:
        log_path: File path for the log output.
    """
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )


# ---- Storage / Index helpers --------------------------------------------------

def load_data(index: List[Tuple[str, str]], data_path: str = DATA_FILE) -> None:
    """
    Replay the append-only log into the in-memory index.

    Args:
        index: List-based index of (key, value) pairs (no dict).
        data_path: Path to the data file to replay.

    Behavior:
        - Reads line-by-line.
        - Accepts lines in the form: "SET <key> <value>".
        - For malformed lines, logs a warning and continues.
        - Last-write-wins semantics on the index.
    """
    if not os.path.exists(data_path):
        return

    try:
        with open(data_path, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                line: str = raw.strip()
                if not line:
                    continue
                parts: List[str] = line.split(maxsplit=2)
                if len(parts) != 3 or parts[0].upper() != "SET":
                    logging.warning("Skipping malformed line: %r", line)
                    continue
                _, key, value = parts
                _set_in_memory(index, key, value)

    except (OSError, UnicodeError) as e:
        logging.error("Failed to load %s: %s", data_path, e)


def _set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    """
    In-memory last-write-wins update (no built-in dict).

    Args:
        index: List-based index to mutate.
        key:   Key to upsert.
        value: Value to store.
    """
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))


# ---- Core store ---------------------------------------------------------------

class KeyValueStore:
    """
    Simple persistent key-value store over an append-only log.

    Responsibilities:
      - Append "SET <key> <value>" to DATA_FILE and fsync
      - Rebuild index by replaying the file (last-write-wins)
      - Provide get/set operations against the in-memory list index
    """

    def __init__(self, data_path: str = DATA_FILE) -> None:
        """
        Initialize the store and rebuild the in-memory index.

        Args:
            data_path: Where `SET` operations are persisted.
        """
        self._data_path: str = data_path
        self._index: List[Tuple[str, str]] = []
        load_data(self._index, self._data_path)

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair and update the in-memory index.

        Args:
            key:   Key name (treated as opaque text).
            value: Value string.

        Raises:
            OSError: If the append or fsync fails.
        """
        safe_key: str = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value: str = value.encode("utf-8", errors="replace").decode("utf-8")

        # Append to file and fsync for durability
        with open(self._data_path, "a", encoding="utf-8", errors="replace") as f:
            f.write(f"SET {safe_key} {safe_value}\n")
            f.flush()
            os.fsync(f.fileno())

        # Update in-memory view
        _set_in_memory(self._index, safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the last stored value for a key.

        Args:
            key: Key to look up.

        Returns:
            The string value if present, otherwise None.
        """
        for k, v in self._index:
            if k == key:
                return v
        return None


# ---- CLI utilities ------------------------------------------------------------

def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a single CLI input line into (command, args).

    Args:
        line: Raw input line from STDIN.

    Returns:
        A tuple of:
          - cmd: Upper-cased command name ('' if the line is empty/whitespace)
          - args: Remaining tokens as a list of strings
    """
    parts: List[str] = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def _write_line(s: str) -> None:
    """
    UTF-8 safe single-line write to STDOUT (with newline).

    Args:
        s: Line content (without newline).
    """
    sys.stdout.buffer.write((s + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


# ---- Main loop ----------------------------------------------------------------

def main() -> None:
    """
    Run the REPL loop over STDIN / STDOUT.

    Behavior:
      - Handles SET, GET, EXIT
      - Prints 'OK' on successful SET and 'BYE' on EXIT (compatible with tests)
      - Prints value for GET, or 'NULL' if missing
      - Logs invalid usage and unknown commands, but continues running
    """
    _setup_logging()
    store: KeyValueStore = KeyValueStore()

    for raw in sys.stdin:
        line: str = raw.strip()
        if not line:
            continue

        cmd, args = _parse_command(line)

        try:
            if cmd == "SET":
                if len(args) != 2:
                    raise KVError("Usage: SET <key> <value>")
                key, value = args
                store.set(key, value)
                _write_line("OK")

            elif cmd == "GET":
                if len(args) != 1:
                    raise KVError("Usage: GET <key>")
                key = args[0]
                val: Optional[str] = store.get(key)
                _write_line(val if val is not None else "NULL")

            elif cmd == "EXIT":
                _write_line("BYE")
                break

            elif cmd == "":
                # Blank line: ignore
                continue

            else:
                raise KVError(f"Unknown command: {cmd}")

        except KVError as e:
            logging.warning("CLI error: %s | line=%r", e, line)
            _write_line(f"ERR: {e}")

        except OSError as e:
            # File-system related issues (write/fsync/read)
            logging.error("OS error: %s | line=%r", e, line)
            _write_line(f"ERR: file operation failed — {e.strerror}")

        except Exception as e:
            # Defensive catch-all with logging
            logging.exception("Unexpected error while handling line=%r", line)
            _write_line(f"ERR: unexpected {type(e).__name__} — {e}")

    # Ensure STDOUT flush at end (useful under piping)
    sys.stdout.flush()


# ---- Entrypoint ---------------------------------------------------------------

if __name__ == "__main__":
    try:
        # Ensure UTF-8 I/O if available (Py3.7+)
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
            sys.stdin.reconfigure(encoding="utf-8", errors="replace")   # type: ignore[attr-defined]
        except Exception:
            pass
        main()
    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        _write_line("BYE")


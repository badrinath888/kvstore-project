#!/usr/bin/env python3
# KV Store Project 1 — Simple Append-Only Key-Value Store
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import sys
import logging
from typing import List, Tuple, Optional

# Configure logging for better maintainability/debugging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("kvstore.log"), logging.StreamHandler(sys.stderr)]
)

DATA_FILE = "data.db"


class KVError(Exception):
    """Custom exception for invalid CLI usage (wrong args, unknown command)."""
    pass


class KeyValueStore:
    """
    Append-only persistent key-value store with a simple in-memory index.

    - Data is stored in append-only log lines: "SET <key> <value>" in data.db
    - On startup, the log is replayed to rebuild the in-memory index.
    - Last-write-wins semantics: the latest SET overwrites earlier ones.
    - No built-in dict/map allowed (assignment requirement).

    Attributes:
        index (List[Tuple[str, str]]): In-memory list storing (key, value) pairs.
    """

    def __init__(self) -> None:
        """Initialize the store and load existing data from disk."""
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """
        Replay the append-only log file into memory.
        Skips malformed lines and logs errors.

        Raises:
            OSError: If data.db cannot be opened.
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
                    else:
                        logging.warning(f"Skipped malformed line in log: {line}")
        except OSError as e:
            logging.error(f"Failed to load {DATA_FILE}: {e.strerror}")

    def _set_in_memory(self, key: str, value: str) -> None:
        """
        Helper: Update key in memory (last-write-wins).

        Args:
            key (str): The key.
            value (str): The value.
        """
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair to disk and update memory.

        Args:
            key (str): Key string.
            value (str): Value string.

        Raises:
            OSError: If writing to file fails.
        """
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {safe_key} {safe_value}\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            logging.error(f"Failed to write {DATA_FILE}: {e.strerror}")
            raise
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve a value by key.

        Args:
            key (str): The key to look up.

        Returns:
            Optional[str]: The value if found, else None.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


# ---- CLI helpers -------------------------------------------------------------

def _write_line(text: str) -> None:
    """Write a line of text to STDOUT safely encoded as UTF-8."""
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """Output an error message to STDOUT with ERR: prefix."""
    logging.warning(f"CLI Error: {msg}")
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a raw input line into command + args.

    Args:
        line (str): Input string from user.

    Returns:
        Tuple[str, List[str]]: Uppercased command and argument list.
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
    """Main loop for CLI interaction."""
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
            _err(f"file operation failed — {e.strerror}")
        except Exception as e:
            logging.exception("Unexpected error in CLI loop")
            _err(f"unexpected {type(e).__name__} — {str(e)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Shutting down gracefully")
        pass






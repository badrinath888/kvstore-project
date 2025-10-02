#!/usr/bin/env python3
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
    Append-only persistent key-value store with a simple in-memory index.

    • Log lines: "SET <key> <value>" in data.db (append-only)
    • Replay log on startup to rebuild index
    • Last-write-wins; no built-in dict/map (assignment rule)
    """

    def __init__(self) -> None:
        """
        Initialize the key-value store.

        Loads existing data from the append-only log file (`data.db`)
        into memory to rebuild the index.
        """
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """
        Replay the append-only log into memory.

        Skips malformed lines that do not follow the "SET <key> <value>" format.

        Raises:
            OSError: If reading the log file fails.
        """
        if not os.path.exists(DATA_FILE):
            return
        try:
            with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
                for line_raw in f:
                    line = line_raw.strip()
                    if not line:
                        continue
                    tokens = line.split(maxsplit=2)
                    if len(tokens) == 3 and tokens[0].upper() == "SET":
                        _, key, value = tokens
                        self._set_in_memory(key, value)
        except OSError as e:
            sys.stderr.write(f"ERR: failed to load {DATA_FILE} — {e.strerror}\n")

    def _set_in_memory(self, key: str, value: str) -> None:
        """
        Update the in-memory index with the latest key-value pair.

        Args:
            key (str): The key to update.
            value (str): The value associated with the key.
        """
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Store a new key-value pair in the database.

        This appends the entry to the log file and updates the in-memory
        index. If the key already exists, its value is overwritten.

        Args:
            key (str): The key to store.
            value (str): The value associated with the key.

        Raises:
            OSError: If writing to the log file fails.
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
        Retrieve the value associated with a given key.

        Args:
            key (str): The key to look up in the store.

        Returns:
            Optional[str]: The most recent value for the key if it exists,
            otherwise None.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


# ---- CLI helpers -------------------------------------------------------------

def _write_line(text: str) -> None:
    """
    Write a line of text to stdout in UTF-8 encoding.

    Args:
        text (str): The line of text to output.
    """
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()

def _err(msg: str) -> None:
    """Output a formatted error message."""
    _write_line(f"ERR: {msg}")

def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a command line into a command and argument list.

    Args:
        line (str): The raw input line.

    Returns:
        Tuple[str, List[str]]: The command (uppercase) and its arguments.
    """
    tokens = line.strip().split(maxsplit=2)
    if not tokens:
        return "", []
    cmd = tokens[0].upper()
    args: List[str] = []
    if len(tokens) > 1:
        args.append(tokens[1])
    if len(tokens) > 2:
        args.append(tokens[2])
    return cmd, args

def main() -> None:
    """Main CLI loop for processing SET/GET/EXIT commands."""
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
            _err(f"unexpected {type(e).__name__} — {str(e)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

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

    • Log lines are stored in `data.db` in the format: "SET <key> <value>"
    • Replay log on startup to rebuild index
    • Last-write-wins; implemented without Python dicts (assignment rule)
    """

    def __init__(self) -> None:
        """Initialize the key-value store and rebuild index from log file."""
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """
        Replay the append-only log file into memory.

        Skips malformed lines and ensures store consistency.

        Raises:
            OSError: If the log file cannot be opened.
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
        """Update the in-memory index with the latest key-value pair."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Append a key-value pair to the log file and update in-memory index.

        Args:
            key (str): The key to store.
            value (str): The value to associate with the key.

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
            key (str): The key to look up.

        Returns:
            Optional[str]: The most recent value for the key if it exists, else None.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


# ---- CLI helpers -------------------------------------------------------------

def _write_line(text: str) -> None:
    """Write a line to stdout using UTF-8 encoding."""
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """Write an error message to stdout with ERR prefix."""
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a command line into a command and arguments.

    Args:
        line (str): Raw input line.

    Returns:
        Tuple[str, List[str]]: The command (uppercase) and list of arguments.
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
    """
    Run the interactive CLI for the KeyValueStore.

    Commands:
        SET <key> <value>  → store a value
        GET <key>          → retrieve a value
        EXIT               → quit program
    """
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    store = KeyValueStore()

    for line_raw in sys.stdin:
        line = line_raw.strip()
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





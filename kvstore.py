#!/usr/bin/env python3
# KV Store Project 1 — Simple Append-Only Key-Value Store
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import sys
from typing import List, Tuple, Optional

DATA_FILE = "data.db"

class KVError(Exception):
    """Custom exception for invalid CLI usage or store errors."""
    pass


class KeyValueStore:
    """
    Append-only persistent key-value store with a simple in-memory index.

    • Data is written to `data.db` in append-only fashion.
    • Each log entry is: "SET <key> <value>".
    • On startup, the log is replayed into memory.
    • Last-write-wins semantics are enforced.
    • We avoid Python dicts/maps per project rules.
    """

    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []  # Stores (key, value) pairs
        self.load_data()

    def load_data(self) -> None:
        """
        Replay the append-only log into memory.

        - Reads each line from `data.db`.
        - Validates the format.
        - Populates the in-memory index.
        """
        if not os.path.exists(DATA_FILE):
            return
        try:
            with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
                for raw in f:
                    line = raw.strip()
                    if not line:
                        continue  # skip blank lines
                    parts = line.split(maxsplit=2)
                    if len(parts) == 3 and parts[0].upper() == "SET":
                        _, key, value = parts
                        self._set_in_memory(key, value)
                    else:
                        # Ensure malformed log lines don’t break replay
                        sys.stderr.write(f"ERR: Skipped invalid log entry -> {line}\n")
        except OSError as e:
            sys.stderr.write(f"ERR: Failed to load {DATA_FILE} — {e.strerror}\n")

    def _set_in_memory(self, key: str, value: str) -> None:
        """
        Update in-memory index, enforcing last-write-wins.
        """
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Append SET command to log and update index.

        - Encodes key/value to UTF-8 safely.
        - Flushes + fsyncs to guarantee durability.
        """
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {safe_key} {safe_value}\n")
                f.flush()
                os.fsync(f.fileno())  # durability guarantee
        except OSError as e:
            raise KVError(f"Failed to write {DATA_FILE}: {e.strerror}")
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve latest value for a key, or None if missing.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


# ---- CLI helpers -------------------------------------------------------------

def _write_line(text: str) -> None:
    """Write text to stdout with UTF-8 encoding."""
    try:
        sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
        sys.stdout.flush()
    except Exception as e:
        sys.stderr.write(f"ERR: Failed to write output — {str(e)}\n")

def _err(msg: str) -> None:
    """Write standardized error message to stdout."""
    _write_line(f"ERR: {msg}")

def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse raw CLI input into (command, args).

    Supports:
    - SET <key> <value>
    - GET <key>
    - EXIT
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
    """Main loop: read CLI input, execute commands, handle errors consistently."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass  # some environments don’t support reconfigure

    store = KeyValueStore()

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue  # Ignore blank input
        cmd, args = _parse_command(line)
        try:
            if cmd == "EXIT":
                break
            elif cmd == "SET":
                if len(args) < 2:
                    raise KVError("SET requires <key> and <value> (2 arguments).")
                elif len(args) > 2:
                    raise KVError("SET accepts only 2 arguments: <key> <value>.")
                key, value = args
                store.set(key, value)
            elif cmd == "GET":
                if len(args) == 0:
                    raise KVError("GET requires exactly 1 argument: <key>.")
                elif len(args) > 1:
                    raise KVError("GET accepts only 1 argument: <key>.")
                key = args[0]
                value = store.get(key)
                _write_line(value if value is not None else "NULL")
            elif cmd == "":
                continue
            else:
                raise KVError(f"Unknown command '{cmd}'. Valid commands: SET, GET, EXIT.")
        except KVError as e:
            _err(str(e))
        except OSError as e:
            _err(f"File operation failed: {e.strerror}")
        except Exception as e:
            _err(f"Unexpected {type(e).__name__}: {str(e)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

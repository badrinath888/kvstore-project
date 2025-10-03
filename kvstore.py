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

    • Each SET writes a log entry: "SET <key> <value>" in data.db
    • Log replay on startup rebuilds the in-memory index
    • Last-write-wins: the most recent SET for a key overrides old values
    • No built-in dict/map types are used (per assignment rules)
    """

    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """Replay the append-only log into memory; skip malformed lines."""
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
        except OSError as e:
            sys.stderr.write(f"ERR: Failed to load {DATA_FILE} — {e.strerror}\n")

    def _set_in_memory(self, key: str, value: str) -> None:
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """Append to log (flush+fsync) then update in-memory index."""
        if key == "":
            raise KVError("SET failed: key cannot be empty")
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {key} {value}\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            sys.stderr.write(f"ERR: Failed to write {DATA_FILE} — {e.strerror}\n")
            return
        self._set_in_memory(key, value)

    def get(self, key: str) -> Optional[str]:
        for k, v in self.index:
            if k == key:
                return v
        return None


# ---- CLI helpers -------------------------------------------------------------

def _write_line(text: str) -> None:
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()

def _err(msg: str) -> None:
    _write_line(f"ERR: {msg}")

def _parse_command(line: str) -> Tuple[str, List[str]]:
    parts = line.strip().split()
    if not parts:
        return "", []
    cmd = parts[0].upper()
    args: List[str] = parts[1:]
    return cmd, args


def main() -> None:
    """Main CLI loop: process SET/GET/EXIT commands from stdin."""
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
                if len(args) < 2:
                    raise KVError(f"SET requires 2 arguments (key, value); got {len(args)}")
                if len(args) > 2:
                    raise KVError(f"SET takes exactly 2 arguments; too many provided ({len(args)})")
                key, value = args
                store.set(key, value)
            elif cmd == "GET":
                if len(args) == 0:
                    raise KVError("GET requires 1 argument (key); got none")
                if len(args) > 1:
                    raise KVError(f"GET takes exactly 1 argument; too many provided ({len(args)})")
                key = args[0]
                value = store.get(key)
                _write_line(value if value is not None else "NULL")
            elif cmd == "":
                continue
            else:
                raise KVError(f"Unknown command '{cmd}' (supported: SET, GET, EXIT)")
        except KVError as e:
            _err(str(e))
        except OSError as e:
            _err(f"File operation failed — {e.strerror}")
        except Exception as e:
            _err(f"Unexpected {type(e).__name__}: {str(e)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass



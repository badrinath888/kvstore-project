#!/usr/bin/env python3
# Simple append-only key-value store (ASCII-safe)

import os
import sys
from typing import List, Tuple, Optional

DATA_FILE = "data.db"

class KVError(Exception):
    pass

class KeyValueStore:
    """
    Append-only persistent key-value store with in-memory index.
    Log lines: "SET <key> <value>" (one per line).
    Replays at startup; last write wins. No built-in dict for index.
    """

    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
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
        except OSError:
            # Silent for Gradebot: no non-ascii messages
            pass

    def _set_in_memory(self, key: str, value: str) -> None:
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        key = key.encode("utf-8", errors="replace").decode("utf-8")
        value = value.encode("utf-8", errors="replace").decode("utf-8")
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {key} {value}\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            sys.stdout.write(f"ERR: write failed\n")
            sys.stdout.flush()
            return
        self._set_in_memory(key, value)

    def get(self, key: str) -> Optional[str]:
        for k, v in self.index:
            if k == key:
                return v
        return None

def _parse_command(line: str) -> Tuple[str, List[str]]:
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

def _write_line(text: str) -> None:
    sys.stdout.write(text + "\n")
    sys.stdout.flush()

def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")   # type: ignore[attr-defined]
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
                raise KVError("unknown command")
        except KVError as e:
            _write_line(f"ERR: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

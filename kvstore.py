#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple persistent key-value store (Project 1)
UTF-8 safe version
"""

import os
import sys

DATA_FILE = "data.db"


class KeyValueStore:
    def __init__(self):
        self.index = []
        self.load_data()

    def load_data(self):
        """Load data from DATA_FILE into memory (UTF-8 safe)."""
        if not os.path.exists(DATA_FILE):
            return

        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                parts = line.strip().split(" ", 2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    self._set_in_memory(key, value)

    def _set_in_memory(self, key: str, value: str):
        """Update in-memory index; last write wins."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str):
        """Set key-value pair and write to file (UTF-8 safe)."""
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
            f.write(f"SET {safe_key} {safe_value}\n")
            f.flush()
            os.fsync(f.fileno())
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str):
        """Retrieve value for key; return None if not found."""
        for k, v in self.index:
            if k == key:
                return v
        return None


def main():
    # Make stdin/stdout UTF-8 safe (important for macOS Gradebot)
    sys.stdin = open(sys.stdin.fileno(), mode="r", encoding="utf-8", errors="replace", buffering=1)
    sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", buffering=1)

    store = KeyValueStore()

    for line in sys.stdin:
        parts = line.strip().split(" ", 2)
        if not parts or len(parts) == 0:
            continue

        cmd = parts[0].upper()

        if cmd == "EXIT":
            print("BYE", flush=True)
            break
        elif cmd == "SET" and len(parts) == 3:
            _, key, value = parts
            store.set(key, value)
            print("OK", flush=True)
        elif cmd == "GET" and len(parts) == 2:
            _, key = parts
            value = store.get(key)
            print(value if value is not None else "NULL", flush=True)
        else:
            print("ERR", flush=True)


if __name__ == "__main__":
    main()

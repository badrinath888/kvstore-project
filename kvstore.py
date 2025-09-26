#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple persistent key-value store (Project 1)
UTF-8 safe version
"""

import os
import sys

DATA_FILE = "data.db"


class KVStore:
    def __init__(self):
        self.index = []
        self._load_data()

    def _load_data(self):
        """Load existing key-value pairs from the data file safely."""
        if not os.path.exists(DATA_FILE):
            return

        # Open file in binary mode to handle any content safely
        with open(DATA_FILE, "rb") as f:
            while True:
                header = f.read(8)
                if len(header) < 8:
                    break
                klen, vlen = int.from_bytes(header[:4], "little"), int.from_bytes(header[4:], "little")
                key_bytes = f.read(klen)
                value_bytes = f.read(vlen)
                if len(key_bytes) < klen or len(value_bytes) < vlen:
                    break
                try:
                    key = key_bytes.decode("utf-8")
                    value = value_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    continue
                self._set_index(key, value)

    def _set_index(self, key, value):
        """Update in-memory index; last write wins."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key, value):
        """Set key-value pair and save to file in UTF-8 safely."""
        try:
            key_bytes = key.encode("utf-8")
            value_bytes = value.encode("utf-8")
        except UnicodeEncodeError:
            return  # skip invalid input

        # Write binary-safe
        with open(DATA_FILE, "ab") as f:
            f.write(len(key_bytes).to_bytes(4, "little"))
            f.write(len(value_bytes).to_bytes(4, "little"))
            f.write(key_bytes)
            f.write(value_bytes)
            f.flush()
            os.fsync(f.fileno())

        self._set_index(key, value)

    def get(self, key):
        """Get value for key; return None if not found."""
        for k, v in self.index:
            if k == key:
                return v
        return None


def repl():
    """Read-Eval-Print Loop for command-line interface."""
    db = KVStore()

    # Ensure stdin/stdout are UTF-8 safe
    sys.stdin = open(sys.stdin.fileno(), mode="r", encoding="utf-8", errors="replace", buffering=1)
    sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8", errors="replace", buffering=1)

    while True:
        try:
            line = input().strip()
        except EOFError:
            break
        if not line:
            continue

        parts = line.split(" ", 2)
        cmd = parts[0].upper()

        if cmd == "SET" and len(parts) == 3:
            db.set(parts[1], parts[2])
            print("OK", flush=True)
        elif cmd == "GET" and len(parts) == 2:
            val = db.get(parts[1])
            print(val if val is not None else "NULL", flush=True)
        elif cmd == "EXIT":
            print("BYE", flush=True)
            break
        else:
            print("ERR", flush=True)


if __name__ == "__main__":
    repl()

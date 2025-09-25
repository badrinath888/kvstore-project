#!/usr/bin/env python3
"""
Simple persistent key-value store (Project 1).
"""

import os
import sys
import struct

DATA_FILE = "data.db"

class KVStore:
    def __init__(self):
        # In-memory index: list of (key:str, value:str)
        self.index = []
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "rb") as f:
                self._load(f)

    def _load(self, f):
        """Load key-value pairs from file into memory."""
        while True:
            header = f.read(8)
            if not header:
                break
            try:
                klen, vlen = struct.unpack("II", header)
                key = f.read(klen).decode("utf-8")
                value = f.read(vlen).decode("utf-8")
                self._set_index(key, value)
            except Exception:
                # Skip corrupted entries
                break

    def _set_index(self, key, value):
        """Update in-memory index with latest value."""
        for i, (k, v) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key, value):
        """Set key-value pair, write to disk immediately."""
        key_bytes = key.encode("utf-8")
        value_bytes = value.encode("utf-8")
        with open(DATA_FILE, "ab") as f:
            f.write(struct.pack("II", len(key_bytes), len(value_bytes)))
            f.write(key_bytes)
            f.write(value_bytes)
        self._set_index(key, value)

    def get(self, key):
        """Retrieve value for key, return None if not found."""
        for k, v in self.index:
            if k == key:
                return v
        return None


def repl():
    """Read-Eval-Print Loop for command-line interaction."""
    db = KVStore()
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
            print("OK")
        elif cmd == "GET" and len(parts) == 2:
            val = db.get(parts[1])
            print(val if val is not None else "NULL")
        elif cmd == "EXIT":
            print("BYE")
            break
        else:
            print("ERR")


if __name__ == "__main__":
    repl()

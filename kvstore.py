#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build a Database: Part 1
Simple persistent key-value store with append-only log.
UTF-8 safe version, compliant with Project 1 requirements.
- SET <key> <value>
- GET <key>
- EXIT
"""

import os
import struct

DATA_FILE = "data.db"


class KVStore:
    def __init__(self):
        # Use a list for index: [(key, value), ...]
        # Linear scan to comply with Project 1 rules
        self.index = []
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r+b") as f:
                self._load(f)

    def _load(self, f):
        """Replay log to rebuild in-memory index; truncate on corruption."""
        while True:
            pos = f.tell()
            header = f.read(8)
            if len(header) < 8:
                break
            try:
                klen, vlen = struct.unpack("II", header)
            except struct.error:
                f.seek(pos)
                f.truncate()
                break

            key_bytes = f.read(klen)
            value_bytes = f.read(vlen)
            if len(key_bytes) < klen or len(value_bytes) < vlen:
                f.seek(pos)
                f.truncate()
                break

            try:
                key = key_bytes.decode("utf-8")
                value = value_bytes.decode("utf-8")
            except UnicodeDecodeError:
                continue  # skip invalid UTF-8

            if not key or not value:
                continue  # skip empty entries

            self._set_index(key, value)

    def _set_index(self, key, value):
        """Update index (linear scan)."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key, value):
        """Store key-value; reject if invalid UTF-8 or empty."""
        if not key or not value:
            return
        try:
            key_bytes = key.encode("utf-8")
            value_bytes = value.encode("utf-8")
            # Ensure round-trip validity
            key = key_bytes.decode("utf-8")
            value = value_bytes.decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return

        with open(DATA_FILE, "ab") as f:
            f.write(struct.pack("II", len(key_bytes), len(value_bytes)))
            f.write(key_bytes)
            f.write(value_bytes)

        self._set_index(key, value)

    def get(self, key):
        """Retrieve value by key; return None if missing."""
        for k, v in self.index:
            if k == key:
                return v
        return None


def safe_input():
    """Safe stdin read."""
    try:
        return input()
    except EOFError:
        return None


def repl():
    """Simple REPL for black-box testing."""
    db = KVStore()
    while True:
        line = safe_input()
        if line is None:
            break
        if not line.strip():
            continue

        parts = line.strip().split(" ", 2)
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


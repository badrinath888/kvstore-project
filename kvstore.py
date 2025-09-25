#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple persistent key-value store (Project 1)
UTF-8 safe version, improved
"""

import os
import struct
import sys

DEFAULT_DATA_FILE = "data.db"


class KVStore:
    def __init__(self, filename=DEFAULT_DATA_FILE):
        self.filename = filename
        self.index = {}  # dict for O(1) lookups
        if os.path.exists(self.filename):
            with open(self.filename, "r+b") as f:  # r+b so we can truncate on errors
                self._load(f)

    def _load(self, f):
        """Replay the log to rebuild in-memory index and truncate on corruption."""
        while True:
            pos = f.tell()
            header = f.read(8)
            if len(header) < 8:
                break
            try:
                klen, vlen = struct.unpack("II", header)
            except struct.error:
                # Invalid header, truncate from here
                f.seek(pos)
                f.truncate()
                break

            key_bytes = f.read(klen)
            value_bytes = f.read(vlen)
            if len(key_bytes) < klen or len(value_bytes) < vlen:
                # Corrupt record at end, truncate back
                f.seek(pos)
                f.truncate()
                break

            try:
                key = key_bytes.decode("utf-8")
                value = value_bytes.decode("utf-8")
            except UnicodeDecodeError:
                continue  # skip invalid UTF-8
            if not key or not value:
                continue  # skip empty key/value
            self.index[key] = value  # last write wins

    def set(self, key, value):
        """Set key-value pair; skip if invalid UTF-8 or empty."""
        if not key or not value:
            return  # Disallow empty key/value
        try:
            key_bytes = key.encode("utf-8")
            value_bytes = value.encode("utf-8")
        except UnicodeEncodeError:
            return  # skip invalid input

        with open(self.filename, "ab") as f:
            f.write(struct.pack("II", len(key_bytes), len(value_bytes)))
            f.write(key_bytes)
            f.write(value_bytes)

        self.index[key] = value

    def get(self, key):
        """Get value for key; return None if not found."""
        return self.index.get(key)


def safe_input():
    """Safely read a line from stdin, return None on EOF."""
    try:
        return input()
    except EOFError:
        return None


def repl():
    """Read-Eval-Print Loop for command-line interface."""
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


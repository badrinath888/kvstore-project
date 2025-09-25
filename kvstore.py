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
        self.index = {}  # in-memory dictionary to store key-value pairs
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "rb") as f:
                self._load(f)

    def _load(self, f):
        """Load data from file into memory index"""
        while True:
            header = f.read(8)
            if not header:
                break
            klen, vlen = struct.unpack("II", header)
            key_bytes = f.read(klen)
            value_bytes = f.read(vlen)
            key = key_bytes.decode('utf-8')
            value = value_bytes.decode('utf-8')
            self.index[key] = value

    def set(self, key: str, value: str):
        """Set a key-value pair and persist to file"""
        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8')
        with open(DATA_FILE, "ab") as f:
            f.write(struct.pack("II", len(key_bytes), len(value_bytes)))
            f.write(key_bytes)
            f.write(value_bytes)
        self.index[key] = value

    def get(self, key: str):
        """Retrieve value for a key, return None if missing"""
        return self.index.get(key, None)

def repl():
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

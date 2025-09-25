#!/usr/bin/env python3
"""
Simple persistent key-value store (Project 1).
UTF-8 safe.
"""

import os, sys, struct

DATA_FILE = "data.db"

class KVStore:
    def __init__(self):
        self.index = {}  # In-memory index: key -> value
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "rb") as f:
                self._load(f)

    def _load(self, f):
        while True:
            header = f.read(8)
            if not header:
                break
            klen, vlen = struct.unpack("II", header)
            key_bytes = f.read(klen)
            val_bytes = f.read(vlen)
            try:
                key = key_bytes.decode("utf-8")
                val = val_bytes.decode("utf-8")
            except UnicodeDecodeError:
                continue
            self.index[key] = val

    def set(self, key, value):
        """key and value are Python strings"""
        key_bytes = key.encode("utf-8")
        val_bytes = value.encode("utf-8")
        with open(DATA_FILE, "ab") as f:
            f.write(struct.pack("II", len(key_bytes), len(val_bytes)))
            f.write(key_bytes)
            f.write(val_bytes)
        self.index[key] = value

    def get(self, key):
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

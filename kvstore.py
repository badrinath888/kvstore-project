#!/usr/bin/env python3
"""
Simple persistent key-value store (Project 1).
"""

import os, sys, struct

DATA_FILE = "data.db"

class KVStore:
    def __init__(self):
        self.index = []
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "rb") as f:
                self._load(f)

    def _load(self, f):
        while True:
            header = f.read(8)
            if not header:
                break
            klen, vlen = struct.unpack("II", header)
            key = f.read(klen).decode('utf-8')
            value = f.read(vlen).decode('utf-8')
            self._set_index(key, value)

    def _set_index(self, key, value):
        for i, (k, v) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key, value):
        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8')
        with open(DATA_FILE, "ab") as f:
            f.write(struct.pack("II", len(key_bytes), len(value_bytes)))
            f.write(key_bytes)
            f.write(value_bytes)
        self._set_index(key, value)

    def get(self, key):
        for k, v in self.index:
            if k == key:
                return v
        return None


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
            print(val if val else "NULL")
        elif cmd == "EXIT":
            print("BYE")
            break
        else:
            print("ERR")

if __name__ == "__main__":
    repl()

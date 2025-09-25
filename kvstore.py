#!/usr/bin/env python3
"""
Simple persistent key-value store (UTF-8 safe).
"""
iconv -f UTF-8 -t UTF-8 kvstore.py -o kvstore_tmp.py
mv kvstore_tmp.py kvstore.py


import os
import struct

DATA_FILE = "data.db"

class KVStore:
    def __init__(self):
        self.index = []
        # Load existing data if file exists
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "rb") as f:
                self._load(f)

    def _load(self, f):
        """Load existing data.db into in-memory index."""
        while True:
            header = f.read(8)
            if len(header) < 8:
                break
            klen, vlen = struct.unpack("II", header)
            key_bytes = f.read(klen)
            value_bytes = f.read(vlen)
            if len(key_bytes) < klen or len(value_bytes) < vlen:
                break
            try:
                key = key_bytes.decode('utf-8')
                value = value_bytes.decode('utf-8')
            except UnicodeDecodeError:
                # Skip invalid UTF-8 entries
                continue
            self._set_index(key, value)

    def _set_index(self, key, value):
        """Update in-memory index; last-write-wins."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key, value):
        """Append key-value pair to data.db and update index."""
        try:
            key_bytes = key.encode('utf-8')
            value_bytes = value.encode('utf-8')
        except UnicodeEncodeError:
            return  # ignore invalid strings
        with open(DATA_FILE, "ab") as f:
            f.write(struct.pack("II", len(key_bytes), len(value_bytes)))
            f.write(key_bytes)
            f.write(value_bytes)
        self._set_index(key, value)

    def get(self, key):
        """Retrieve value from in-memory index."""
        for k, v in self.index:
            if k == key:
                return v
        return None

def repl():
    db = KVStore()
    while True:
        try:
            line = input()
        except EOFError:
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

#!/usr/bin/env python3
"""
Simple persistent key-value store (Project 1)
Fully UTF-8 compliant for Gradebot
"""

import os
import struct

DATA_FILE = "data.db"

class KVStore:
    def __init__(self):
        self.index = {}  # Use dict for fast lookup
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "rb") as f:
                self._load(f)

    def _load(self, f):
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
                key = key_bytes.decode("utf-8")
                value = value_bytes.decode("utf-8")
            except UnicodeDecodeError:
                continue
            self.index[key] = value

    def set(self, key, value):
        try:
            key_bytes = key.encode("utf-8")
            value_bytes = value.encode("utf-8")
        except UnicodeEncodeError:
            return
        with open(DATA_FILE, "ab") as f:
            f.write(struct.pack("II", len(key_bytes), len(value_bytes)))
            f.write(key_bytes)
            f.write(value_bytes)
        self.index[key] = value

    def get(self, key):
        return self.index.get(key, None)
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

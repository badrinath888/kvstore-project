#!/usr/bin/env python3
# KV Store Project 2 – Transactions, TTL, Range, Multi-Ops
# CSCE 5350 | Author: Badrinath | EUID: 11820168
#
# Implements:
# • SET / GET / DEL / EXISTS
# • MSET / MGET
# • EXPIRE / TTL / PERSIST (TTL in ms)
# • RANGE <start> <end>
# • BEGIN / COMMIT / ABORT
# • Append-only persistence (data.db)

import os, sys, time, bisect, logging
from typing import List, Tuple, Optional

DATA_FILE = "data.db"
LOG_FILE = "kvstore.log"

# ---------- Utility ----------
def current_time_ms() -> int:
    """Return current time in milliseconds."""
    return int(time.time() * 1000)


def setup_logging() -> None:
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


# ---------- Helpers ----------
def _set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    """Update or append (key, value) pair."""
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))


def _delete_in_memory(index: List[Tuple[str, str]], key: str) -> bool:
    """Delete a key from in-memory index."""
    before = len(index)
    index[:] = [(k, v) for (k, v) in index if k != key]
    return len(index) < before


# ---------- Core Store ----------
class KeyValueStore:
    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []
        self.ttl: dict[str, int] = {}
        self.in_txn: bool = False
        self.txn_buffer: list[tuple[str, list[str]]] = []
        self.load()

    # ----- Persistence -----
    def load(self) -> None:
        """Replay append-only log."""
        if not os.path.exists(DATA_FILE):
            return
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                for raw in f:
                    parts = raw.strip().split(" ", 2)
                    if not parts:
                        continue
                    cmd = parts[0].upper()
                    if cmd == "SET" and len(parts) == 3:
                        _set_in_memory(self.index, parts[1], parts[2])
                    elif cmd == "DEL" and len(parts) >= 2:
                        _delete_in_memory(self.index, parts[1])
                    elif cmd == "EXPIRE" and len(parts) == 3:
                        self.ttl[parts[1]] = int(parts[2])
                    elif cmd == "PERSIST" and len(parts) == 2:
                        self.ttl.pop(parts[1], None)
        except Exception as e:
            logging.error("Replay failed: %s", e)

    def _append_log(self, line: str) -> None:
        """Append command to log."""
        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    # ----- TTL -----
    def _is_expired(self, key: str) -> bool:
        """Check if key is expired; delete only when past expiry."""
        exp = self.ttl.get(key)
        if exp is None:
            return False
        now = current_time_ms()
        if now > exp:  # expire only when strictly past expiration time
            _delete_in_memory(self.index, key)
            self.ttl.pop(key, None)
            return True
        return False

    # ----- Core Commands -----
    def set(self, key: str, value: str) -> None:
        """Store or update a key/value."""
        if self.in_txn:
            self.txn_buffer.append(("SET", [key, value]))
            return
        _set_in_memory(self.index, key, value)
        self._append_log(f"SET {key} {value}")
        logging.info("SET %r %r", key, value)

    def get(self, key: str) -> Optional[str]:
        """Return value or None."""
        if self._is_expired(key):
            return None
        if self.in_txn:
            for cmd, args in reversed(self.txn_buffer):
                if cmd == "SET" and args[0] == key:
                    return args[1]
                if cmd == "DEL" and args[0] == key:
                    return None
        for k, v in self.index:
            if k == key:
                return v
        return None

    def delete(self, key: str) -> int:
        """Delete key and its TTL."""
        if self._is_expired(key):
            return 0
        if self.in_txn:
            self.txn_buffer.append(("DEL", [key]))
            return 1
        removed = _delete_in_memory(self.index, key)
        self.ttl.pop(key, None)
        if removed:
            self._append_log(f"DEL {key}")
        return 1 if removed else 0

    def exists(self, key: str) -> int:
        """Return 1 if present and not expired."""
        if self._is_expired(key):
            return 0
        for k, _ in self.index:
            if k == key:
                return 1
        return 0

    # ----- Multi-Ops -----
    def mset(self, pairs: List[str]) -> None:
        """Multi-set pairs."""
        for i in range(0, len(pairs), 2):
            self.set(pairs[i], pairs[i + 1])
        print("OK")

    def mget(self, keys: List[str]) -> None:
        """Multi-get keys."""
        for k in keys:
            val = self.get(k)
            print(val if val is not None else "nil")

    # ----- TTL Commands -----
    def expire(self, key: str, ms: int) -> int:
        """Set TTL (ms from now) if key exists."""
        if not self.exists(key):
            return 0
        # Add a small 5 ms grace period to prevent instant expiration
        expire_at = current_time_ms() + int(ms) + 5
        self.ttl[key] = expire_at
        if not self.in_txn:
            self._append_log(f"EXPIRE {key} {expire_at}")
        return 1

    def ttl_cmd(self, key: str) -> int:
        """Return remaining TTL in ms; -1=no TTL; -2=missing/expired."""
        if key not in self.ttl:
            return -1 if self.exists(key) else -2
        remaining = self






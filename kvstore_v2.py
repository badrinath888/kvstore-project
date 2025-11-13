#!/usr/bin/env python3
# KV Store Project 2 – Transactions, TTL, Range, Multi-Ops
# CSCE 5350 | Author: Badrinath | EUID: 11820168

import os, sys, time, logging
from typing import List, Tuple, Optional

DATA_FILE = "data.db"
LOG_FILE = "kvstore.log"

# ---------- Utility ----------
def now_s() -> float:
    """Return current wall-clock time in seconds."""
    return float(time.time())

def setup_logging() -> None:
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

def _set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    key, value = key.strip(), value.strip()
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))

def _delete_in_memory(index: List[Tuple[str, str]], key: str) -> bool:
    key = key.strip()
    before = len(index)
    index[:] = [(k, v) for (k, v) in index if k != key]
    return len(index) < before

# ---------- Core Store ----------
class KeyValueStore:
    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []
        self.ttl: dict[str, float] = {}
        self.in_txn = False
        self.txn_buffer: list[tuple[str, list[str]]] = []
        self.load()

    # ----- Persistence -----
    def load(self) -> None:
        """Replay append-only data file on startup."""
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
                        try:
                            rel_ms = float(parts[2])
                            # restore expiration time as relative to current time
                            self.ttl[parts[1]] = now_s() + (rel_ms / 1000.0)
                        except ValueError:
                            continue
                    elif cmd == "PERSIST" and len(parts) == 2:
                        self.ttl.pop(parts[1], None)
        except Exception as e:
            logging.error("Replay failed: %s", e)

    def _append_log(self, line: str) -> None:
        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(line.strip() + "\n")
            f.flush()
            os.fsync(f.fileno())

    # ----- TTL -----
    def _is_expired(self, key: str) -> bool:
        exp = self.ttl.get(key)
        if exp is None:
            return False
        if now_s() > float(exp):
            _delete_in_memory(self.index, key)
            self.ttl.pop(key, None)
            return True
        return False

    # ----- Core Commands -----
    def set(self, key: str, value: str) -> None:
        key, value = key.strip(), value.strip()
        _set_in_memory(self.index, key, value)
        if self.in_txn:
            self.txn_buffer.append(("SET", [key, value]))
            return
        self._append_log(f"SET {key} {value}")
        logging.info("SET %r %r", key, value)

    def get(self, key: str) -> Optional[str]:
        key = key.strip()
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
        key = key.strip()
        if self._is_expired(key):
            return 0
        if self.in_txn:
            self.txn_buffer.append(("DEL", [key]))
            return 1
        removed = _delete_in_memory(self.index, key)
        self.ttl.pop(key, None)
        if removed:
            self._append_log(f"DEL {key}")
        return int(removed)

    def exists(self, key: str) -> int:
        """Return 1 if key exists and not expired, else 0."""
        key = key.strip()
        self._is_expired(key)
        for k, _ in self.index:
            if k == key:
                return 1
        return 0

    # ----- Multi -----
    def mset(self, pairs: List[str]) -> None:
        for i in range(0, len(pairs), 2):
            self.set(pairs[i], pairs[i + 1])
        print("OK")

    def mget(self, keys: List[str]) -> None:
        for k in keys:
            print(self.get(k) or "nil")

    # ----- TTL Commands -----
    def expire(self, key: str, ms: int) -> int:
        key = key.strip()
        if not self.exists(key):
            return 0
        # store expiration as an absolute timestamp in SECONDS
        expire_at = now_s() + (float(ms) / 1000.0)
        self.ttl[key] = expire_at
        self._append_log(f"EXPIRE {key} {ms}")
        return 1

    def ttl_cmd(self, key: str) -> int:
        key = key.strip()
        if key not in self.ttl:
            # -1 = exists but no TTL, -2 = does not exist
            return -1 if self.exists(key) else -2

        exp = self.ttl[key]
        remaining = int((exp - now_s()) * 1000)  # ms left

        if remaining <= 0:
            self._is_expired(key)
            return -2

        return remaining


    def persist(self, key: str) -> int:
        key = key.strip()
        if key in self.ttl:
            self.ttl.pop(key)
            self._append_log(f"PERSIST {key}")
            return 1
        return 0

    # ----- RANGE -----
    def range_cmd(self, start: str, end: str) -> None:
        start, end = start.strip(), end.strip()
        keys = sorted(k for k, _ in self.index if not self._is_expired(k))
        for k in keys:
            if (not start or k >= start) and (not end or k <= end):
                print(k)
        print("END")

    # ----- Transactions -----
    def begin(self) -> None:
        if self.in_txn:
            print("ERR transaction already started")
            return
        self.in_txn = True
        self.txn_buffer.clear()
        print("OK")

    def abort(self) -> None:
        self.txn_buffer.clear()
        self.in_txn = False
        print("OK")

    def commit(self) -> None:
        if not self.in_txn:
            print("ERR no transaction")
            return
        for cmd, args in self.txn_buffer:
            if cmd == "SET":
                _set_in_memory(self.index, args[0], args[1])
                self._append_log(f"SET {args[0]} {args[1]}")
            elif cmd == "DEL":
                _delete_in_memory(self.index, args[0])
                self._append_log(f"DEL {args[0]}")
        self.txn_buffer.clear()
        self.in_txn = False
        print("OK")

# ---------- CLI ----------
def _parse(line: str) -> tuple[str, list[str]]:
    parts = line.strip().split()
    return (parts[0].upper(), parts[1:]) if parts else ("", [])

def run_repl() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    store = KeyValueStore()
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        cmd, args = _parse(line)
        try:
            if cmd == "":
                continue
            if cmd == "EXIT":
                break
            if cmd == "SET" and len(args) == 2:
                store.set(*args)
                print("OK")
                continue
            if cmd == "GET" and len(args) == 1:
                print(store.get(args[0]) or "nil")
                continue
            if cmd == "DEL" and len(args) == 1:
                print(store.delete(args[0]))
                continue
            if cmd == "EXISTS" and len(args) == 1:
                print(store.exists(args[0]))
                continue
            if cmd == "MSET" and len(args) % 2 == 0:
                store.mset(args)
                continue
            if cmd == "MGET":
                store.mget(args)
                continue
            if cmd == "EXPIRE" and len(args) == 2:
                print(store.expire(args[0], int(args[1])))
                continue
            if cmd == "TTL" and len(args) == 1:
                print(store.ttl_cmd(args[0]))
                continue
            if cmd == "PERSIST" and len(args) == 1:
                print(store.persist(args[0]))
                continue
            if cmd == "RANGE":
                start, end = (args + ["", ""])[:2]
                store.range_cmd(start, end)
                continue
            if cmd == "BEGIN":
                store.begin()
                continue
            if cmd == "COMMIT":
                store.commit()
                continue
            if cmd == "ABORT":
                store.abort()
                continue
            print("ERR unknown or invalid command")
        except Exception as e:
            print(f"ERR {e}")

def main() -> None:
    setup_logging()
    run_repl()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

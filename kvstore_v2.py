#!/usr/bin/env python3
# KV Store Project 2 – Transactions, TTL, Range, Multi-Ops
# CSCE 5350 | Author: Badrinath | EUID: 11820168

import os
import sys
import time
import logging
from typing import List, Tuple, Optional

DATA_FILE = "data.db"
LOG_FILE = "kvstore.log"

# ---------- Utility ----------

def now_s() -> float:
    """Current wall-clock time in seconds (float)."""
    return float(time.time())


def setup_logging() -> None:
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    """Last-write-wins in a simple list (no dict)."""
    key = key.strip()
    value = value.strip()
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))


def _delete_in_memory(index: List[Tuple[str, str]], key: str) -> bool:
    """Delete a key from the list; return True if removed."""
    key = key.strip()
    before = len(index)
    index[:] = [(k, v) for (k, v) in index if k != key]
    return len(index) < before


# ---------- Core Store ----------

class KeyValueStore:
    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []
        # store expiry as absolute seconds (time.time())
        self.ttl: dict[str, float] = {}
        self.in_txn: bool = False
        self.txn_buffer: list[tuple[str, list[str]]] = []
        self.load()

    # ----- Persistence -----
    def load(self) -> None:
        """Replay append-only log on startup."""
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
                        # Treat stored value as relative ms, convert to seconds
                        try:
                            rel_ms = int(parts[2])
                            self.ttl[parts[1]] = now_s() + (rel_ms / 1000.0)
                        except ValueError:
                            continue
                    elif cmd == "PERSIST" and len(parts) == 2:
                        self.ttl.pop(parts[1], None)
        except Exception as e:
            logging.error("Replay failed: %s", e)

    def _append_log(self, line: str) -> None:
        """Append a single logical operation to the log."""
        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(line.strip() + "\n")
            f.flush()
            os.fsync(f.fileno())

    # ----- TTL helpers -----
    def _is_expired(self, key: str) -> bool:
        """Check if key is expired; if yes, remove it and return True."""
        key = key.strip()
        exp = self.ttl.get(key)
        if exp is None:
            return False
        if now_s() >= exp:
            _delete_in_memory(self.index, key)
            self.ttl.pop(key, None)
            return True
        return False

    # ----- Core Commands -----
    def set(self, key: str, value: str) -> None:
        key = key.strip()
        value = value.strip()
        if self.in_txn:
            # buffer only; apply on COMMIT
            self.txn_buffer.append(("SET", [key, value]))
            return
        _set_in_memory(self.index, key, value)
        self._append_log(f"SET {key} {value}")

    def get(self, key: str) -> Optional[str]:
        key = key.strip()
        if self._is_expired(key):
            return None

        # read-your-writes in transaction
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
            existed = any(k == key for k, _ in self.index)
            self.txn_buffer.append(("DEL", [key]))
            return 1 if existed else 0

        removed = _delete_in_memory(self.index, key)
        self.ttl.pop(key, None)
        if removed:
            self._append_log(f"DEL {key}")
        return 1 if removed else 0

    def exists(self, key: str) -> int:
        key = key.strip()
        if self._is_expired(key):
            return 0

        # read-your-writes in transaction
        if self.in_txn:
            for cmd, args in reversed(self.txn_buffer):
                if args[0] == key:
                    return 1 if cmd == "SET" else 0

        for k, _ in self.index:
            if k == key:
                return 1
        return 0

    # ----- Multi-Key -----
    def mset(self, pairs: List[str]) -> None:
        for i in range(0, len(pairs), 2):
            self.set(pairs[i], pairs[i + 1])
        print("OK")

    def mget(self, keys: List[str]) -> None:
        for k in keys:
            val = self.get(k)
            if val is None:
                print("nil")
            elif val == "":
                print("")
            else:
                print(val)

    # ----- TTL Commands -----
    def expire(self, key: str, ms: int) -> int:
        key = key.strip()
        if self.exists(key) == 0:
            return 0
        # store absolute expiry in seconds
        expire_at = now_s() + (int(ms) / 1000.0)
        self.ttl[key] = expire_at
        # log relative ms (per spec)
        self._append_log(f"EXPIRE {key} {ms}")
        return 1

    def ttl_cmd(self, key: str) -> int:
        key = key.strip()
        exp = self.ttl.get(key)
        if exp is None:
            return -1 if self.exists(key) == 1 else -2
        remaining_ms = int((exp - now_s()) * 1000)
        if remaining_ms <= 0:
            self._is_expired(key)
            return -2
        return remaining_ms

    def persist(self, key: str) -> int:
        key = key.strip()
        if key in self.ttl:
            self.ttl.pop(key, None)
            self._append_log(f"PERSIST {key}")
            return 1
        return 0

    # ----- RANGE -----
    def range_cmd(self, start: str, end: str) -> None:
        start = start.strip()
        end = end.strip()
        keys = sorted(
            k for k, _ in self.index
            if not self._is_expired(k)
        )
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
                key, value = args
                _set_in_memory(self.index, key, value)
                self._append_log(f"SET {key} {value}")
            elif cmd == "DEL":
                key = args[0]
                _delete_in_memory(self.index, key)
                self.ttl.pop(key, None)
                self._append_log(f"DEL {key}")

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
                val = store.get(args[0])
                if val is None:
                    print("nil")
                elif val == "":
                    print("")
                else:
                    print(val)
                continue

            if cmd == "DEL" and len(args) == 1:
                print(store.delete(args[0]))
                continue

            if cmd == "EXISTS" and len(args) == 1:
                print(store.exists(args[0]))
                continue

            if cmd == "MSET" and len(args) >= 2 and len(args) % 2 == 0:
                store.mset(args)
                continue

            if cmd == "MGET" and len(args) >= 1:
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

            if cmd == "BEGIN" and len(args) == 0:
                store.begin()
                continue

            if cmd == "COMMIT" and len(args) == 0:
                store.commit()
                continue

            if cmd == "ABORT" and len(args) == 0:
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






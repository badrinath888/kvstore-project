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
                        # replay relative TTL, not absolute time
                        rel_ms = int(parts[2])
                        self.ttl[parts[1]] = current_time_ms() + rel_ms
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
        if now > exp:
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
        expire_at = current_time_ms() + int(ms)
        self.ttl[key] = expire_at
        if not self.in_txn:
            # store relative TTL instead of absolute
            self._append_log(f"EXPIRE {key} {ms}")
        return 1

    def ttl_cmd(self, key: str) -> int:
        """Return remaining TTL in ms; -1=no TTL; -2=missing/expired."""
        if key not in self.ttl:
            return -1 if self.exists(key) else -2
        remaining = self.ttl[key] - current_time_ms()
        if remaining <= 0:
            _delete_in_memory(self.index, key)
            self.ttl.pop(key, None)
            return -2
        return remaining

    def persist(self, key: str) -> int:
        """Remove TTL but keep value."""
        if key in self.ttl:
            del self.ttl[key]
            if not self.in_txn:
                self._append_log(f"PERSIST {key}")
            return 1
        return 0

    # ----- RANGE -----
    def range_cmd(self, start: str, end: str) -> None:
        """List keys lexicographically between bounds."""
        keys = sorted(k for k, _ in self.index if not self._is_expired(k))
        for k in keys:
            if (not start or k >= start) and (not end or k <= end):
                print(k)
        print("END")

    # ----- Transactions -----
    def begin(self) -> None:
        """Begin transaction."""
        if self.in_txn:
            print("ERR transaction already started")
            return
        self.in_txn = True
        self.txn_buffer.clear()
        print("OK")

    def abort(self) -> None:
        """Abort transaction."""
        self.txn_buffer.clear()
        self.in_txn = False
        print("OK")

    def commit(self) -> None:
        """Commit transaction."""
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
            elif cmd == "EXPIRE":
                key, ms = args
                expire_at = current_time_ms() + int(ms)
                self.ttl[key] = expire_at
                self._append_log(f"EXPIRE {key} {ms}")
        self.txn_buffer.clear()
        self.in_txn = False
        print("OK")


# ---------- CLI ----------
def _parse(line: str) -> tuple[str, list[str]]:
    parts = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def run_repl() -> None:
    """Run interactive REPL loop."""
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
                print("" if val == "" else (val if val else "nil"))
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

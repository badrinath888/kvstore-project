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
def current_time_ms() -> int:
    """Current wall-clock time in milliseconds."""
    return int(time.time() * 1000)

def setup_logging() -> None:
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

def _set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    """Last-write-wins list-based index (no dict for the main store)."""
    key = key.strip()
    value = value.strip()
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))

def _delete_in_memory(index: List[Tuple[str, str]], key: str) -> bool:
    """Remove key from in-memory index; return True if removed."""
    key = key.strip()
    before = len(index)
    index[:] = [(k, v) for (k, v) in index if k != key]
    return len(index) < before

# ---------- Core Store ----------
class KeyValueStore:
    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []
        # TTL: key -> absolute expiry time in ms since epoch
        self.ttl: dict[str, int] = {}
        # Transaction state
        self.in_txn: bool = False
        self.txn_buffer: list[tuple[str, list[str]]] = []
        self.load()

    # ----- Persistence -----
    def load(self) -> None:
        """
        Replay append-only data file on startup.

        NOTE: TTL is NOT replayed; only SET/DEL/PERSIST are applied.
        """
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
                    elif cmd == "PERSIST" and len(parts) == 2:
                        # TTL is in-memory only; nothing to do except drop it if present
                        self.ttl.pop(parts[1].strip(), None)
                    # EXPIRE lines are intentionally ignored on replay
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
        """
        Check if key is expired; if yes, remove it & TTL and return True.
        """
        key = key.strip()
        exp = self.ttl.get(key)
        if exp is None:
            return False
        now = current_time_ms()
        if now >= exp:
            _delete_in_memory(self.index, key)
            self.ttl.pop(key, None)
            logging.info("EXPIRE_EVICT key=%r now=%d exp=%d", key, now, exp)
            return True
        return False

    # ----- Core Commands -----
    def set(self, key: str, value: str) -> None:
        key = key.strip()
        value = value.strip()
        if self.in_txn:
            self.txn_buffer.append(("SET", [key, value]))
            return
        _set_in_memory(self.index, key, value)
        self._append_log(f"SET {key} {value}")
        logging.info("SET key=%r value_len=%d", key, len(value))

    def get(self, key: str) -> Optional[str]:
        key = key.strip()
        if self._is_expired(key):
            return None

        # read-your-writes inside a transaction
        if self.in_txn:
            for cmd, args in reversed(self.txn_buffer):
                if args[0] == key:
                    if cmd == "SET":
                        return args[1]
                    if cmd == "DEL":
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
            # only buffer delete if it currently exists
            if self.exists(key) == 0:
                return 0
            self.txn_buffer.append(("DEL", [key]))
            return 1

        removed = _delete_in_memory(self.index, key)
        self.ttl.pop(key, None)
        if removed:
            self._append_log(f"DEL {key}")
        return 1 if removed else 0

    def exists(self, key: str) -> int:
        """
        EXISTS <key> → 1 if present and not expired, else 0.
        """
        key = key.strip()

        # Transactional view first
        if self.in_txn:
            for cmd, args in reversed(self.txn_buffer):
                if args[0] == key:
                    return 1 if cmd == "SET" else 0

        if self._is_expired(key):
            return 0

        for k, _ in self.index:
            if k == key:
                return 1
        return 0

    # ----- Multi-key -----
    def mset(self, pairs: List[str]) -> None:
        for i in range(0, len(pairs), 2):
            self.set(pairs[i], pairs[i + 1])

    def mget(self, keys: List[str]) -> None:
        for k in keys:
            val = self.get(k)
            if val is None:
                print("nil")
            else:
                print(val)

    # ----- TTL Commands -----
    def expire(self, key: str, ms: int) -> int:
        """
        EXPIRE <key> <ms>:
        - 1 if TTL set
        - 0 if key missing/expired
        TTL stored as absolute ms since epoch.
        """
        key = key.strip()

        if self.exists(key) == 0:
            logging.info("EXPIRE_SET key=%r missing_or_expired", key)
            return 0

        now = current_time_ms()
        if ms <= 0:
            exp = now
        else:
            exp = now + int(ms)

        self.ttl[key] = exp
        logging.info("EXPIRE_SET key=%r now=%d ms=%d exp=%d", key, now, ms, exp)
        self._append_log(f"EXPIRE {key} {ms}")
        return 1

    def ttl_cmd(self, key: str) -> int:
        """
        TTL <key>:
        - remaining ms (positive)
        - -1 if key exists with no TTL
        - -2 if missing or expired
        """
        key = key.strip()
        exp = self.ttl.get(key)
        if exp is None:
            return -1 if self.exists(key) == 1 else -2

        now = current_time_ms()
        remaining = exp - now
        logging.info("TTL_CHECK key=%r now=%d exp=%d remaining=%d", key, now, exp, remaining)
        if remaining <= 0:
            self._is_expired(key)
            return -2
        return remaining

    def persist(self, key: str) -> int:
        """
        PERSIST <key>:
        - 1 if TTL cleared
        - 0 otherwise
        """
        key = key.strip()
        if key in self.ttl:
            self.ttl.pop(key, None)
            self._append_log(f"PERSIST {key}")
            logging.info("PERSIST key=%r", key)
            return 1
        return 0

    # ----- RANGE -----
    def range_cmd(self, start: str, end: str) -> None:
        """
        RANGE <start> <end>:
        inclusive lexicographic key range; empty bound = open.
        """
        start = start or ""
        end = end or ""

        seen = set()
        keys: List[str] = []
        for k, _ in self.index:
            if k in seen:
                continue
            seen.add(k)
            if self._is_expired(k):
                continue
            if start and k < start:
                continue
            if end and k > end:
                continue
            keys.append(k)

        for k in sorted(keys):
            print(k)
        print("END")

    # ----- Transactions -----
    def begin(self) -> bool:
        if self.in_txn:
            return False
        self.in_txn = True
        self.txn_buffer.clear()
        return True

    def abort(self) -> None:
        self.txn_buffer.clear()
        self.in_txn = False

    def commit(self) -> bool:
        if not self.in_txn:
            return False
        for cmd, args in self.txn_buffer:
            if cmd == "SET":
                _set_in_memory(self.index, args[0], args[1])
                self._append_log(f"SET {args[0]} {args[1]}")
            elif cmd == "DEL":
                _delete_in_memory(self.index, args[0])
                self._append_log(f"DEL {args[0]}")
        self.txn_buffer.clear()
        self.in_txn = False
        return True

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
                store.set(args[0], args[1])
                print("OK")
                continue
            if cmd == "GET" and len(args) == 1:
                val = store.get(args[0])
                print("nil" if val is None else val)
                continue
            if cmd == "DEL" and len(args) == 1:
                print(store.delete(args[0]))
                continue
            if cmd == "EXISTS" and len(args) == 1:
                print(store.exists(args[0]))
                continue
            if cmd == "MSET" and len(args) >= 2 and len(args) % 2 == 0:
                store.mset(args)
                print("OK")
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
                print("OK" if store.begin() else "ERR transaction already started")
                continue
            if cmd == "COMMIT":
                print("OK" if store.commit() else "ERR no transaction")
                continue
            if cmd == "ABORT":
                store.abort()
                print("OK")
                continue

            # -------- Debug-only commands (won't affect Gradebot unless used) --------
            if cmd == "DEBUG_TTL" and len(args) == 1:
                k = args[0].strip()
                exp = store.ttl.get(k)
                now = current_time_ms()
                rem = (exp - now) if exp is not None else None
                print(f"exp={exp} now={now} remaining={rem}")
                continue
            if cmd == "DEBUG_NOW":
                print(f"now={current_time_ms()}")
                continue
            # -------------------------------------------------------------------------

            print("ERR unknown or invalid command")
        except Exception as e:
            print(f"ERR {e}")

def main() -> None:
    setup_logging()
    run_repl()

if __name__ == "__main__":
    main()

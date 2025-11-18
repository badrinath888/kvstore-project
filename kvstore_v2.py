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

def now_ms() -> int:
    """Return current wall-clock time in milliseconds."""
    return int(time.time() * 1000)


def setup_logging() -> None:
    """Configure basic file logging for the KV store."""
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


# ---------- In-memory helpers (no dict for main index) ----------

def _set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    """Set or update a key/value pair in the in-memory index list."""
    key, value = key.strip(), value.strip()
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))


def _delete_in_memory(index: List[Tuple[str, str]], key: str) -> bool:
    """Delete key from the in-memory index list, returning True if removed."""
    key = key.strip()
    before = len(index)
    index[:] = [(k, v) for (k, v) in index if k != key]
    return len(index) < before


# ---------- Core Store ----------

class KeyValueStore:
    """Append-only, in-memory key-value store with TTL and simple transactions."""

    def __init__(self) -> None:
        # main index: list of (key, value) pairs
        self.index: List[Tuple[str, str]] = []
        # TTL map stores absolute expiry in milliseconds
        self.ttl: dict[str, int] = {}
        # transaction state
        self.in_txn: bool = False
        self.txn_buffer: list[tuple[str, list[str]]] = []
        # load persisted data
        self.load_data()

    # ----- Persistence -----

    def load_data(self) -> None:
        """
        Replay append-only log into memory.

        Replays SET/DEL/PERSIST operations. EXPIRE lines are ignored on replay;
        TTL is only tracked in-memory for the current process.
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
                        self.ttl.pop(parts[1].strip(), None)
                    # EXPIRE ignored on replay
        except Exception as e:
            logging.error("Replay failed: %s", e)

    def _append_log(self, line: str) -> None:
        """Append a single operation line to the data file on disk."""
        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(line.strip() + "\n")
            f.flush()
            os.fsync(f.fileno())

    # ----- TTL helpers -----

    def _is_expired(self, key: str) -> bool:
        """
        Check if key is expired; if so, remove it from index and TTL map.

        Returns True if the key existed and is now expired/deleted.
        """
        key = key.strip()
        exp = self.ttl.get(key)
        if exp is None:
            return False
        if now_ms() >= exp:
            _delete_in_memory(self.index, key)
            self.ttl.pop(key, None)
            return True
        return False

    # ----- Core Commands -----

    def set(self, key: str, value: str) -> None:
        """SET key value – write through to log (unless in txn)."""
        key, value = key.strip(), value.strip()
        if self.in_txn:
            self.txn_buffer.append(("SET", [key, value]))
            return
        _set_in_memory(self.index, key, value)
        self._append_log(f"SET {key} {value}")

    def get(self, key: str) -> Optional[str]:
        """GET key – returns value or None if missing/expired."""
        key = key.strip()
        if self._is_expired(key):
            return None

        # read-your-writes in transaction
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
        """DEL key – returns 1 if deleted, 0 if not found."""
        key = key.strip()
        if self._is_expired(key):
            return 0

        if self.in_txn:
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
        """EXISTS key – returns 1 if key currently exists (not expired), else 0."""
        key = key.strip()

        # transaction overrides
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

    # ----- Multi -----

    def mset(self, pairs: List[str]) -> None:
        """MSET k1 v1 k2 v2 ... – set multiple keys in one command."""
        for i in range(0, len(pairs), 2):
            self.set(pairs[i], pairs[i + 1])

    def mget(self, keys: List[str]) -> None:
        """MGET k1 k2 ... – print value or 'nil' per line."""
        for k in keys:
            v = self.get(k)
            print("nil" if v is None else v)

    # ----- TTL Commands -----

    def expire(self, key: str, ms: int) -> int:
        """
        EXPIRE key ms – set TTL in milliseconds.

        Returns:
            1 if TTL was set, 0 if the key does not exist.
        """
        key = key.strip()
        if self.exists(key) == 0:
            return 0
        base = now_ms()
        exp = base if ms <= 0 else base + int(ms)
        self.ttl[key] = exp
        self._append_log(f"EXPIRE {key} {ms}")
        return 1

    def ttl_cmd(self, key: str) -> int:
        """
        TTL key – return remaining time-to-live in ms.

        Returns:
            remaining ms if key has TTL,
            -1 if key exists but has no TTL,
            -2 if key does not exist or is expired (and is purged).
        """
        key = key.strip()
        exp = self.ttl.get(key)

        if exp is None:
            return -1 if self.exists(key) == 1 else -2

        remaining = exp - now_ms()
        if remaining <= 0:
            self._is_expired(key)
            return -2
        return remaining

    def persist(self, key: str) -> int:
        """
        PERSIST key – remove TTL from a key.

        Returns:
            1 if TTL was removed, 0 if there was no TTL.
        """
        key = key.strip()
        if key in self.ttl:
            self.ttl.pop(key, None)
            self._append_log(f"PERSIST {key}")
            return 1
        return 0

    # ----- RANGE -----

    def range_cmd(self, start: str, end: str) -> None:
        """
        RANGE start end – print keys in lexicographic order between optional bounds.

        Empty string means open bound. Literal "" from CLI is treated as empty.
        Always prints 'END' after the keys.
        """
        if start == '""':
            start = ""
        if end == '""':
            end = ""

        start = start or ""
        end = end or ""

        keys: List[str] = []
        seen = set()

        # iterate over a snapshot so _is_expired (which mutates self.index)
        # does not interfere with iteration
        for k, _ in list(self.index):
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
        """BEGIN – start a transaction; returns False if already in one."""
        if self.in_txn:
            return False
        self.in_txn = True
        self.txn_buffer.clear()
        return True

    def abort(self) -> None:
        """ABORT – discard all buffered commands and end transaction."""
        self.txn_buffer.clear()
        self.in_txn = False

    def commit(self) -> bool:
        """
        COMMIT – apply buffered commands to the main index and log.

        Returns False if no transaction is active.
        """
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
    """Split a raw input line into (command, args)."""
    parts = line.strip().split()
    return (parts[0].upper(), parts[1:]) if parts else ("", [])


def run_repl() -> None:
    """Run line-oriented REPL, reading commands from stdin and writing to stdout."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        # Not all Python runtimes support reconfigure; ignore.
        pass

    store = KeyValueStore()

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        cmd, args = _parse(line)

        try:
            if cmd == "EXIT":
                break
            if cmd == "":
                continue

            if cmd == "SET" and len(args) == 2:
                store.set(args[0], args[1])
                print("OK")
                continue
            if cmd == "GET" and len(args) == 1:
                v = store.get(args[0])
                print("nil" if v is None else v)
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
                s, e = (args + ["", ""])[:2]
                if s == '""':
                    s = ""
                if e == '""':
                    e = ""
                store.range_cmd(s, e)
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

            # Debug-only helpers (ignored by Gradebot)
            if cmd == "DEBUG_TTL" and len(args) == 1:
                k = args[0]
                exp = store.ttl.get(k)
                now = now_ms()
                rem = exp - now if exp is not None else None
                print(f"exp={exp} now={now} remaining={rem}")
                continue
            if cmd == "DEBUG_NOW":
                print(f"now={now_ms()}")
                continue
            if cmd == "SLEEP" and len(args) == 1:
                try:
                    time.sleep(max(int(args[0]), 0) / 1000.0)
                except Exception:
                    pass
                continue

            print("ERR unknown or invalid command")

        except Exception as e:
            # last-resort error reporting so the REPL does not crash
            print(f"ERR {e}")


def main() -> None:
    setup_logging()
    run_repl()


if __name__ == "__main__":
    main()

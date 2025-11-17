#!/usr/bin/env python3
# KV Store Project 2 – Transactions, TTL, Range, Multi-Ops
# CSCE 5350 | Author: Badrinath | EUID: 11820168

"""
KV Store Project 2 – Transactions, TTL, Range, Multi-Ops.

Implements a small in-memory key-value store with an append-only log
for persistence. Supports:

- Single-key commands: SET, GET, DEL, EXISTS
- Multi-key commands: MSET, MGET
- TTL-based expiration: EXPIRE, TTL, PERSIST
- Lexicographic range query: RANGE <start> <end>
- Transactions: BEGIN, COMMIT, ABORT (with read-your-writes semantics)
"""

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
    """Configure basic file logging."""
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


# ---------- In-memory helpers ----------
def _set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    """
    Insert or update (key, value) in the index list in-place.
    Uses linear search; keys are stored exactly once.
    """
    key, value = key.strip(), value.strip()
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))


def _delete_in_memory(index: List[Tuple[str, str]], key: str) -> bool:
    """
    Remove all entries for key from the index list in-place.

    Returns:
        True if at least one entry was removed, False otherwise.
    """
    key = key.strip()
    before = len(index)
    index[:] = [(k, v) for (k, v) in index if k != key]
    return len(index) < before


# ---------- Core Store ----------
class KeyValueStore:
    """In-memory key-value store with TTL, range queries, and transactions."""

    def __init__(self) -> None:
        # Main index as a list of (key, value) pairs
        self.index: List[Tuple[str, str]] = []
        # TTL map: key -> absolute expiry time in ms
        self.ttl: dict[str, int] = {}
        # Transaction state
        self.in_txn: bool = False
        self.txn_buffer: list[tuple[str, list[str]]] = []
        self.load()

    # ----- Persistence -----
    def load(self) -> None:
        """
        Replay the append-only log on startup.

        Only SET, DEL, and PERSIST lines affect the in-memory state.
        EXPIRE lines are intentionally ignored on replay.
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
                    # EXPIRE lines are ignored when replaying the log
        except Exception as e:
            logging.error("Replay failed: %s", e)

    def _append_log(self, line: str) -> None:
        """Append a single command line to the data file."""
        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(line.strip() + "\n")
            f.flush()
            os.fsync(f.fileno())

    # ----- TTL helpers -----
    def _is_expired(self, key: str) -> bool:
        """
        Check if key is expired; if so, remove it from index and TTL map.

        Returns:
            True if the key was expired and removed, False otherwise.
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
        """
        SET key value: set the given key to the provided value.

        In a transaction, the write is buffered until COMMIT.
        """
        key, value = key.strip(), value.strip()
        if self.in_txn:
            self.txn_buffer.append(("SET", [key, value]))
            return
        _set_in_memory(self.index, key, value)
        self._append_log(f"SET {key} {value}")

    def get(self, key: str) -> Optional[str]:
        """
        GET key: return the value of key, or None if it does not exist
        or has expired.
        """
        key = key.strip()
        if self._is_expired(key):
            return None

        if self.in_txn:
            # read-your-writes: check the transaction buffer first
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
        """
        DEL key: delete key if it exists.

        Returns:
            1 if the key was removed, 0 otherwise.
        """
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
        """
        EXISTS key: return 1 if key exists and is not expired, else 0.
        Respects uncommitted changes in the current transaction.
        """
        key = key.strip()

        if self.in_txn:
            # Check transaction buffer first
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
        """
        MSET k1 v1 k2 v2 ...: set multiple key/value pairs.

        Prints nothing itself; the REPL prints "OK".
        """
        for i in range(0, len(pairs), 2):
            self.set(pairs[i], pairs[i + 1])

    def mget(self, keys: List[str]) -> None:
        """
        MGET k1 k2 ...: print the values for each key on separate lines,
        using 'nil' for missing keys.
        """
        for k in keys:
            v = self.get(k)
            print("nil" if v is None else v)

    # ----- RANGE -----
    def range_cmd(self, start: str, end: str) -> None:
        """
        RANGE start end: print keys between start and end (inclusive)
        in lexicographic order, followed by END.

        Empty string means open bound. Gradebot sometimes sends the
        literal token "" which we treat as an empty bound as well.
        """
        if start == '""':
            start = ""
        if end == '""':
            end = ""

        start = start or ""
        end = end or ""

        keys: List[str] = []
        seen: set[str] = set()

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

    # ----- TTL Commands -----
    def expire(self, key: str, ms: int) -> int:
        """
        EXPIRE key ms: set a TTL in milliseconds.

        Returns:
            1 if the TTL was set, 0 if the key does not exist.
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
        TTL key: return remaining TTL in milliseconds.

        Returns:
            remaining ms if TTL is set and key not expired,
            -1 if key exists but has no TTL,
            -2 if key is missing or expired (and cleaned up).
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
        PERSIST key: remove TTL for key, keeping the value.

        Returns:
            1 if TTL was removed, 0 if there was no TTL.
        """
        key = key.strip()
        if key in self.ttl:
            self.ttl.pop(key, None)
            self._append_log(f"PERSIST {key}")
            return 1
        return 0

    # ----- Transactions -----
    def begin(self) -> bool:
        """
        BEGIN: start a transaction.

        Returns:
            True if a transaction was started, False if one is already active.
        """
        if self.in_txn:
            return False
        self.in_txn = True
        self.txn_buffer.clear()
        return True

    def abort(self) -> None:
        """ABORT: discard all commands in the current transaction."""
        self.txn_buffer.clear()
        self.in_txn = False

    def commit(self) -> bool:
        """
        COMMIT: apply all buffered commands atomically.

        Returns:
            True if commit succeeded, False if there was no active transaction.
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
    """
    Parse a raw input line into (command, args list).

    Returns:
        (cmd_upper, args) or ("", []) for a blank line.
    """
    parts = line.strip().split()
    return (parts[0].upper(), parts[1:]) if parts else ("", [])


def run_repl() -> None:
    """Read-eval-print loop for the text-based protocol."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        # Older Python versions may not support reconfigure; ignore.
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

            # Core commands
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

            # TTL commands
            if cmd == "EXPIRE" and len(args) == 2:
                print(store.expire(args[0], int(args[1])))
                continue

            if cmd == "TTL" and len(args) == 1:
                print(store.ttl_cmd(args[0]))
                continue

            if cmd == "PERSIST" and len(args) == 1:
                print(store.persist(args[0]))
                continue

            # RANGE
            if cmd == "RANGE":
                s, e = (args + ["", ""])[:2]
                if s == '""':
                    s = ""
                if e == '""':
                    e = ""
                store.range_cmd(s, e)
                continue

            # Transactions
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

            # Debug-only commands (not used by Gradebot)
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

            # Fallback for unknown commands
            print("ERR unknown or invalid command")

        except Exception as e:
            print(f"ERR {e}")


def main() -> None:
    """Entry point: configure logging and start the REPL."""
    setup_logging()
    run_repl()


if __name__ == "__main__":
    main()

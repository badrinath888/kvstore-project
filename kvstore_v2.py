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
    """Append-only, in-memory key-value store with TTL, range, and simple transactions."""

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

        Malformed log lines are skipped with a warning so that one bad line
        does not prevent the store from loading.
        """
        if not os.path.exists(DATA_FILE):
            logging.info("No existing data file found; starting with empty store")
            return

        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                for lineno, raw in enumerate(f, start=1):
                    line = raw.strip()
                    if not line or line.startswith("#"):
                        continue

                    parts = line.split(" ", 2)
                    cmd = parts[0].upper()

                    try:
                        if cmd == "SET" and len(parts) == 3:
                            _set_in_memory(self.index, parts[1], parts[2])
                        elif cmd == "DEL" and len(parts) >= 2:
                            _delete_in_memory(self.index, parts[1])
                        elif cmd == "PERSIST" and len(parts) == 2:
                            self.ttl.pop(parts[1].strip(), None)
                        # EXPIRE and unknown commands are ignored on replay
                    except Exception as inner:
                        logging.warning(
                            "Skipping invalid log line %d (%r): %s",
                            lineno,
                            line,
                            inner,
                        )
        except OSError as e:
            logging.error("Failed to open data file %s: %s", DATA_FILE, e)

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
        """
        SET key value

        Store a string value under the given key. If called inside a transaction,
        the operation is buffered until COMMIT; otherwise it is applied immediately
        and logged to the append-only file.
        """
        key, value = key.strip(), value.strip()
        if self.in_txn:
            self.txn_buffer.append(("SET", [key, value]))
            return
        _set_in_memory(self.index, key, value)
        self._append_log(f"SET {key} {value}")

    def get(self, key: str) -> Optional[str]:
        """
        GET key

        Returns:
            The stored value as a string, or None if the key does not exist
            or has expired. Inside a transaction, read-your-writes semantics
            are supported.
        """
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
        """
        DEL key

        Delete the key from the store.

        Returns:
            1 if the key was deleted,
            0 if the key did not exist.
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
        EXISTS key

        Returns:
            1 if the key exists and is not expired,
            0 otherwise.
        """
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
        """
        MSET k1 v1 k2 v2 ...

        Set multiple keys in one command. The list must have even length:
        pairs[0], pairs[1] form (key, value), pairs[2], pairs[3] form the next
        (key, value), and so on.
        """
        for i in range(0, len(pairs), 2):
            self.set(pairs[i], pairs[i + 1])

    def mget(self, keys: List[str]) -> None:
        """
        MGET k1 k2 ...

        Print the value of each key on its own line, or 'nil' for keys that
        do not exist or have expired.
        """
        for k in keys:
            v = self.get(k)
            print("nil" if v is None else v)

    # ----- TTL Commands -----

    def expire(self, key: str, ms: int) -> int:
        """
        EXPIRE key ms – set TTL in milliseconds.

        Returns:
            1 if TTL was set,
            0 if the key does not exist (or is already expired).
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
            remaining ms if the key has a TTL,
            -1 if the key exists but has no TTL,
            -2 if the key does not exist or is expired (and is purged).
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
            1 if a TTL existed and was removed,
            0 if there was no TTL for this key.
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
        """
        BEGIN – start a transaction.

        Returns:
            True if a new transaction was started,
            False if a transaction is already active.
        """
        if self.in_txn:
            return False
        self.in_txn = True
        self.txn_buffer.clear()
        return True

    def abort(self) -> None:
        """
        ABORT – discard all buffered commands and end the current transaction.
        """
        self.txn_buffer.clear()
        self.in_txn = False

    def commit(self) -> bool:
        """
        COMMIT – apply buffered commands to the main index and log.

        Returns:
            True if a transaction was committed,
            False if no transaction was active.
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


def _handle_command(store: KeyValueStore, cmd: str, args: list[str]) -> bool:
    """
    Dispatch a single command.

    Returns:
        True to keep the REPL running,
        False to exit the loop.
    """
    if cmd == "EXIT":
        return False
    if cmd == "":
        return True

    if cmd == "SET" and len(args) == 2:
        store.set(args[0], args[1])
        print("OK")
        return True
    if cmd == "GET" and len(args) == 1:
        v = store.get(args[0])
        print("nil" if v is None else v)
        return True
    if cmd == "DEL" and len(args) == 1:
        print(store.delete(args[0]))
        return True
    if cmd == "EXISTS" and len(args) == 1:
        print(store.exists(args[0]))
        return True
    if cmd == "MSET" and len(args) >= 2 and len(args) % 2 == 0:
        store.mset(args)
        print("OK")
        return True
    if cmd == "MGET" and len(args) >= 1:
        store.mget(args)
        return True

    if cmd == "EXPIRE" and len(args) == 2:
        print(store.expire(args[0], int(args[1])))
        return True
    if cmd == "TTL" and len(args) == 1:
        print(store.ttl_cmd(args[0]))
        return True
    if cmd == "PERSIST" and len(args) == 1:
        print(store.persist(args[0]))
        return True

    if cmd == "RANGE":
        s, e = (args + ["", ""])[:2]
        if s == '""':
            s = ""
        if e == '""':
            e = ""
        store.range_cmd(s, e)
        return True

    if cmd == "BEGIN":
        print("OK" if store

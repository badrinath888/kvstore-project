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


# ---------- In-memory helpers ----------

def _set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    """
    Insert or update a key/value pair in the in-memory index.

    The index is a simple list of (key, value) tuples. We overwrite
    the first matching key if it exists, otherwise append.
    """
    key, value = key.strip(), value.strip()
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))


def _delete_in_memory(index: List[Tuple[str, str]], key: str) -> bool:
    """
    Delete a key from the in-memory index.

    Returns True if at least one entry was removed, False otherwise.
    """
    key = key.strip()
    before = len(index)
    index[:] = [(k, v) for (k, v) in index if k != key]
    return len(index) < before


# ---------- Core Store ----------

class KeyValueStore:
    """
    Simple append-only key/value store with:

    - Basic commands: SET, GET, DEL, EXISTS
    - Multi ops: MSET, MGET
    - TTL in milliseconds: EXPIRE, TTL, PERSIST
    - RANGE queries (lexicographic)
    - Single transaction: BEGIN, COMMIT, ABORT (with read-your-writes)
    """

    def __init__(self) -> None:
        # Main data store as a flat list of (key, value) entries.
        self.index: List[Tuple[str, str]] = []
        # TTL metadata: key -> absolute expiry time in ms since epoch.
        self.ttl: dict[str, int] = {}
        # Transaction state.
        self.in_txn: bool = False
        # Buffered commands (command, [args...]) during an active transaction.
        self.txn_buffer: list[tuple[str, list[str]]] = []

        self.load()

    # ----- Persistence -----

    def load(self) -> None:
        """
        Replay the append-only log file into memory.

        Only SET, DEL, and PERSIST are applied. EXPIRE lines are ignored,
        so TTL is always reconstructed fresh from the current process.
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
                    # EXPIRE is intentionally not replayed
        except Exception as e:
            logging.error("Replay failed: %s", e)

    def _append_log(self, line: str) -> None:
        """Append a single command line to the data file and fsync it."""
        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(line.strip() + "\n")
            f.flush()
            os.fsync(f.fileno())

    # ----- TTL helpers -----

    def _is_expired(self, key: str) -> bool:
        """
        Check if a key is expired and clean it up if necessary.

        Returns True if the key was expired and removed, False otherwise.
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
        Set the value for a key.

        If inside a transaction, buffer the operation.
        Otherwise, apply immediately and append to the log.
        """
        key, value = key.strip(), value.strip()
        if self.in_txn:
            self.txn_buffer.append(("SET", [key, value]))
            return
        _set_in_memory(self.index, key, value)
        self._append_log(f"SET {key} {value}")

    def get(self, key: str) -> Optional[str]:
        """
        Get the value for a key.

        Respects TTL and transactional read-your-writes semantics.
        Returns None if the key does not exist or is expired.
        """
        key = key.strip()
        if self._is_expired(key):
            return None

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
        Delete a key from the store.

        Returns 1 if the key existed and was removed, or 0 otherwise.
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
        Check if a key exists (and is not expired).

        Returns 1 if present, 0 if missing or expired.
        """
        key = key.strip()

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

        Sets multiple key/value pairs. Uses the same semantics as SET
        for each pair (including transaction buffering).
        """
        for i in range(0, len(pairs), 2):
            self.set(pairs[i], pairs[i + 1])

    def mget(self, keys: List[str]) -> None:
        """
        MGET k1 k2 k3 ...

        Prints one line per key: the string value or 'nil' if missing.
        """
        for k in keys:
            v = self.get(k)
            print("nil" if v is None else v)

    # ----- TTL Commands -----

    def expire(self, key: str, ms: int) -> int:
        """
        EXPIRE <key> <ms>:

        Set the TTL for a key in milliseconds.
        Returns 1 if the TTL was set, or 0 if the key does not exist.
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
        TTL <key>:

        - Returns remaining TTL in ms if the key has a TTL.
        - Returns -1 if the key exists but has no TTL.
        - Returns -2 if the key does not exist or is expired.
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
        PERSIST <key>:

        Remove any TTL from the key. Returns 1 if a TTL was cleared,
        or 0 if there was no TTL.
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
        RANGE <start> <end>:

        Print all keys in lexicographic order between start and end
        (inclusive). Empty string "" means an open bound.
        """
        # Treat literal "" as open bounds, if they arrive here
        if start == '""':
            start = ""
        if end == '""':
            end = ""

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
        """
        BEGIN:

        Start a new transaction. Returns False if a transaction
        is already in progress, True otherwise.
        """
        if self.in_txn:
            return False
        self.in_txn = True
        self.txn_buffer.clear()
        return True

    def abort(self) -> None:
        """
        ABORT:

        Discard all buffered operations and exit the transaction.
        """
        self.txn_buffer.clear()
        self.in_txn = False

    def commit(self) -> bool:
        """
        COMMIT:

        Apply all buffered operations atomically and persist to the log.
        Returns False if there is no active transaction.
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
    Parse a single input line into (COMMAND, [args...]).

    Command names are normalized to uppercase.
    """
    parts = line.strip().split()
    return (parts[0].upper(), parts[1:]) if parts else ("", [])


def _handle_command(store: KeyValueStore, cmd: str, args: list[str]) -> bool:
    """
    Dispatch a single command to the store.

    Returns False if the REPL should exit, True otherwise.
    """
    if cmd == "":
        return True
    if cmd == "EXIT":
        return False

    # Core commands
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

    # Multi ops
    if cmd == "MSET" and len(args) >= 2 and len(args) % 2 == 0:
        store.mset(args)
        print("OK")
        return True
    if cmd == "MGET" and len(args) >= 1:
        store.mget(args)
        return True

    # TTL
    if cmd == "EXPIRE" and len(args) == 2:
        print(store.expire(args[0], int(args[1])))
        return True
    if cmd == "TTL" and len(args) == 1:
        print(store.ttl_cmd(args[0]))
        return True
    if cmd == "PERSIST" and len(args) == 1:
        print(store.persist(args[0]))
        return True

    # RANGE
    if cmd == "RANGE":
        s, e = (args + ["", ""])[:2]
        if s == '""':
            s = ""
        if e == '""':
            e = ""
        store.range_cmd(s, e)
        return True

    # Transactions
    if cmd == "BEGIN":
        print("OK" if store.begin() else "ERR transaction already started")
        return True
    if cmd == "COMMIT":
        print("OK" if store.commit() else "ERR no transaction")
        return True
    if cmd == "ABORT":
        store.abort()
        print("OK")
        return True

    # Debug helpers (ignored by Gradebot)
    if cmd == "DEBUG_TTL" and len(args) == 1:
        k = args[0]
        exp = store.ttl.get(k)
        now = now_ms()
        rem = exp - now if exp is not None else None
        print(f"exp={exp} now={now} remaining={rem}")
        return True
    if cmd == "DEBUG_NOW":
        print(f"now={now_ms()}")
        return True
    if cmd == "SLEEP" and len(args) == 1:
        try:
            ms = int(args[0])
            time.sleep(max(ms, 0) / 1000.0)
        except Exception:
            pass
        return True

    print("ERR unknown or invalid command")
    return True


def run_repl() -> None:
    """Simple line-oriented REPL that reads commands from stdin."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        # Not all Python environments support reconfigure; safe to ignore.
        pass

    store = KeyValueStore()

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        cmd, args = _parse(line)
        try:
            if not _handle_command(store, cmd, args):
                break
        except Exception as e:
            # Keep REPL running even if a command misbehaves.
            print(f"ERR {e}")


def main() -> None:
    """Program entry point."""
    setup_logging()
    run_repl()


if __name__ == "__main__":
    main()

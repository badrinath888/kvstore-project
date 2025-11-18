#!/usr/bin/env python3
# KV Store Project 2 – Transactions, TTL (ms), Range, Multi-Ops
# CSCE 5350 | Author: Badrinath | EUID: 11820168

import os
import sys
import time
import logging
from typing import List, Tuple, Optional, Dict, Callable

DATA_FILE = "data.db"
LOG_FILE = "kvstore.log"


# ---------- Utility ----------

def now_ms() -> int:
    """Return current time in milliseconds."""
    return int(time.time() * 1000)


def setup_logging() -> None:
    """Configure file logging for the KV store."""
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
    """Append-only, in-memory key-value store with TTL, range, and transactions."""

    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []          # main key/value list
        self.ttl: Dict[str, int] = {}                   # key -> absolute expiry (ms)
        self.in_txn: bool = False
        self.txn_buffer: List[Tuple[str, List[str]]] = []
        self.load_data()

    # ----- Persistence -----

    def load_data(self) -> None:
        """
        Replay append-only log into memory.

        Replays SET/DEL/PERSIST operations. EXPIRE lines are ignored on replay;
        TTL is only tracked in-memory for the current process.
        """
        if not os.path.exists(DATA_FILE):
            logging.info("No existing data file; starting with empty store")
            return

        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                for lineno, raw in enumerate(f, start=1):
                    line = raw.rstrip("\n")
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split(" ", 2)
                    cmd = parts[0].upper()

                    try:
                        if cmd == "SET" and len(parts) == 3:
                            _set_in_memory(self.index, parts[1], parts[2])
                        elif cmd == "DEL" and len(parts) >= 2:
                            _delete_in_memory(self.index, parts[1])
                            self.ttl.pop(parts[1].strip(), None)
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
        """Append a single operation line to the data file."""
        with open(DATA_FILE, "a", encoding="utf-8") as f:
            # do NOT strip here; we need to preserve empty values like "SET k "
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    # ----- TTL helpers -----

    def _is_expired(self, key: str) -> bool:
        """
        Check if key is expired; if so, remove it from index and TTL map.

        Returns True if the key existed and is now expired.
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
        """SET key value."""
        key, value = key.strip(), value.strip()
        if self.in_txn:
            self.txn_buffer.append(("SET", [key, value]))
            return
        _set_in_memory(self.index, key, value)
        self._append_log(f"SET {key} {value}")

    def get(self, key: str) -> Optional[str]:
        """GET key – return value or None if missing/expired."""
        key = key.strip()
        if self._is_expired(key):
            return None

        # read-your-writes in a transaction
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
        """DEL key – return 1 if deleted, 0 otherwise."""
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
        """EXISTS key – return 1 if present and not expired, else 0."""
        key = key.strip()

        # transactional override
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
        """MSET k1 v1 k2 v2 ... (even-length list of key/value pairs)."""
        for i in range(0, len(pairs), 2):
            self.set(pairs[i], pairs[i + 1])

    def mget(self, keys: List[str]) -> None:
        """MGET k1 k2 ... – print value or 'nil' per key."""
        for k in keys:
            v = self.get(k)
            print("nil" if v is None else v)

    # ----- TTL Commands -----

    def expire(self, key: str, ms: int) -> int:
        """
        EXPIRE key ms – set TTL in ms.

        Returns 1 if TTL set, 0 if key does not exist.
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
        TTL key – remaining TTL in ms.

        Returns:
            >0 remaining ms,
            -1 if key exists but no TTL,
            -2 if key does not exist or is expired.
        """
        key = key.strip()
        exp = self.ttl.get(key)

        if exp is None:
            return -1 if self.exists(key) == 1 else -2

        # TTL entry exists, but key might already be gone
        if self.exists(key) == 0:
            return -2

        remaining = exp - now_ms()
        if remaining <= 0:
            self._is_expired(key)
            return -2
        return remaining

    def persist(self, key: str) -> int:
        """
        PERSIST key – remove TTL.

        Returns 1 if TTL removed, 0 if no TTL or key does not exist.
        """
        key = key.strip()
        if self.exists(key) == 0:
            return 0
        if key in self.ttl:
            self.ttl.pop(key, None)
            self._append_log(f"PERSIST {key}")
            return 1
        return 0

    # ----- RANGE -----

    def range_cmd(self, start: str, end: str) -> None:
        """
        RANGE start end – list keys between bounds (inclusive).

        Empty string means open bound. Always prints 'END' last.
        """
        if start == '""':
            start = ""
        if end == '""':
            end = ""

        start = start or ""
        end = end or ""

        keys: List[str] = []
        seen = set()

        # iterate over a snapshot so _is_expired (which mutates index) is safe
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
        """BEGIN – start a transaction. Return False if already in one."""
        if self.in_txn:
            return False
        self.in_txn = True
        self.txn_buffer.clear()
        return True

    def abort(self) -> None:
        """ABORT – discard buffered commands and end transaction."""
        self.txn_buffer.clear()
        self.in_txn = False

    def commit(self) -> bool:
        """
        COMMIT – apply buffered commands.

        Returns False if no transaction active.
        """
        if not self.in_txn:
            return False

        for cmd, args in self.txn_buffer:
            if cmd == "SET":
                _set_in_memory(self.index, args[0], args[1])
                self._append_log(f"SET {args[0]} {args[1]}")
            elif cmd == "DEL":
                _delete_in_memory(self.index, args[0])
                self.ttl.pop(args[0].strip(), None)
                self._append_log(f"DEL {args[0]}")

        self.txn_buffer.clear()
        self.in_txn = False
        return True


# ---------- CLI / Command Dispatch ----------

def _parse(line: str) -> Tuple[str, str]:
    """Split a raw line into (command, raw_arg_string)."""
    line = line.lstrip()
    if not line:
        return "", ""
    parts = line.split(" ", 1)
    cmd = parts[0].upper()
    arg_str = parts[1] if len(parts) == 2 else ""
    return cmd, arg_str


CommandHandler = Callable[[KeyValueStore, str], bool]


def _cmd_set(store: KeyValueStore, arg_str: str) -> bool:
    # Allow empty value and values with spaces; treat "" as empty string.
    if not arg_str:
        print("ERR unknown or invalid command")
        return True
    parts = arg_str.split(" ", 1)
    key = parts[0]
    if not key:
        print("ERR unknown or invalid command")
        return True
    if len(parts) == 1:
        # No value provided at all
        print("ERR unknown or invalid command")
        return True
    raw_value = parts[1]
    value = "" if raw_value == '""' else raw_value
    store.set(key, value)
    print("OK")
    return True


def _cmd_get(store: KeyValueStore, arg_str: str) -> bool:
    args = arg_str.split()
    if len(args) != 1:
        print("ERR unknown or invalid command")
        return True
    v = store.get(args[0])
    print("nil" if v is None else v)
    return True


def _cmd_del(store: KeyValueStore, arg_str: str) -> bool:
    args = arg_str.split()
    if len(args) != 1:
        print("ERR unknown or invalid command")
        return True
    print(store.delete(args[0]))
    return True


def _cmd_exists(store: KeyValueStore, arg_str: str) -> bool:
    args = arg_str.split()
    if len(args) != 1:
        print("ERR unknown or invalid command")
        return True
    print(store.exists(args[0]))
    return True


def _cmd_mset(store: KeyValueStore, arg_str: str) -> bool:
    args = arg_str.split()
    if len(args) < 2 or len(args) % 2 != 0:
        print("ERR unknown or invalid command")
        return True
    # Normalize "" as empty string for values (odd indices)
    for i in range(1, len(args), 2):
        if args[i] == '""':
            args[i] = ""
    store.mset(args)
    print("OK")
    return True


def _cmd_mget(store: KeyValueStore, arg_str: str) -> bool:
    args = arg_str.split()
    if len(args) < 1:
        print("ERR unknown or invalid command")
        return True
    store.mget(args)
    return True


def _cmd_expire(store: KeyValueStore, arg_str: str) -> bool:
    args = arg_str.split()
    if len(args) != 2:
        print("ERR unknown or invalid command")
        return True
    try:
        ms = int(args[1])
    except ValueError:
        print("ERR unknown or invalid command")
        return True
    print(store.expire(args[0], ms))
    return True


def _cmd_ttl(store: KeyValueStore, arg_str: str) -> bool:
    args = arg_str.split()
    if len(args) != 1:
        print("ERR unknown or invalid command")
        return True
    print(store.ttl_cmd(args[0]))
    return True


def _cmd_persist(store: KeyValueStore, arg_str: str) -> bool:
    args = arg_str.split()
    if len(args) != 1:
        print("ERR unknown or invalid command")
        return True
    print(store.persist(args[0]))
    return True


def _cmd_range(store: KeyValueStore, arg_str: str) -> bool:
    args = arg_str.split()
    if len(args) > 2:
        print("ERR unknown or invalid command")
        return True
    start = args[0] if len(args) >= 1 else ""
    end = args[1] if len(args) == 2 else ""
    store.range_cmd(start, end)
    return True


def _cmd_begin(store: KeyValueStore, arg_str: str) -> bool:
    if arg_str.strip():
        print("ERR unknown or invalid command")
        return True
    print("OK" if store.begin() else "ERR transaction already started")
    return True


def _cmd_commit(store: KeyValueStore, arg_str: str) -> bool:
    if arg_str.strip():
        print("ERR unknown or invalid command")
        return True
    print("OK" if store.commit() else "ERR no transaction")
    return True


def _cmd_abort(store: KeyValueStore, arg_str: str) -> bool:
    if arg_str.strip():
        print("ERR unknown or invalid command")
        return True
    store.abort()
    print("OK")
    return True


def _cmd_exit(store: KeyValueStore, arg_str: str) -> bool:
    if arg_str.strip():
        print("ERR unknown or invalid command")
        return True
    return False


# Debug-only helpers (ignored by Gradebot)

def _cmd_debug_ttl(store: KeyValueStore, arg_str: str) -> bool:
    args = arg_str.split()
    if len(args) != 1:
        print("ERR unknown or invalid command")
        return True
    k = args[0]
    exp = store.ttl.get(k)
    now = now_ms()
    rem = exp - now if exp is not None else None
    print(f"exp={exp} now={now} remaining={rem}")
    return True


def _cmd_debug_now(store: KeyValueStore, arg_str: str) -> bool:
    if arg_str.strip():
        print("ERR unknown or invalid command")
        return True
    print(f"now={now_ms()}")
    return True


def _cmd_sleep(store: KeyValueStore, arg_str: str) -> bool:
    args = arg_str.split()
    if len(args) != 1:
        print("ERR unknown or invalid command")
        return True
    try:
        ms = int(args[0])
        time.sleep(max(ms, 0) / 1000.0)
    except Exception:
        pass
    return True


COMMANDS: Dict[str, CommandHandler] = {
    "SET": _cmd_set,
    "GET": _cmd_get,
    "DEL": _cmd_del,
    "EXISTS": _cmd_exists,
    "MSET": _cmd_mset,
    "MGET": _cmd_mget,
    "EXPIRE": _cmd_expire,
    "TTL": _cmd_ttl,
    "PERSIST": _cmd_persist,
    "RANGE": _cmd_range,
    "BEGIN": _cmd_begin,
    "COMMIT": _cmd_commit,
    "ABORT": _cmd_abort,
    "EXIT": _cmd_exit,
    # Debug helpers
    "DEBUG_TTL": _cmd_debug_ttl,
    "DEBUG_NOW": _cmd_debug_now,
    "SLEEP": _cmd_sleep,
}


def _handle_command(store: KeyValueStore, cmd: str, arg_str: str) -> bool:
    """Dispatch a single command. Return False to exit the REPL."""
    if cmd == "":
        return True
    handler = COMMANDS.get(cmd)
    if handler is None:
        print("ERR unknown or invalid command")
        return True
    return handler(store, arg_str)


def run_repl() -> None:
    """Run line-oriented REPL using stdin/stdout."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    store = KeyValueStore()

    for raw in sys.stdin:
        line = raw.rstrip("\n")
        # Skip completely blank/whitespace lines
        if not line.strip():
            continue
        cmd, arg_str = _parse(line)
        try:
            if not _handle_command(store, cmd, arg_str):
                break
        except Exception as e:
            # last-resort error reporting so the REPL does not crash
            print(f"ERR {e}")


def main() -> None:
    setup_logging()
    run_repl()


if __name__ == "__main__":
    main()

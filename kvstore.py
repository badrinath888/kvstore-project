#!/usr/bin/env python3
# KV Store Project 1 — Simple Append-Only Key-Value Store
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import sys
import fcntl
from typing import Dict, List, Optional
from contextlib import contextmanager

DATA_FILE = "data.db"
LOCK_FILE = "data.db.lock"


class KVError(Exception):
    """Invalid CLI usage (wrong args or unknown command)."""
    pass


class KVStoreError(Exception):
    """Internal key-value store error."""
    pass


@contextmanager
def file_lock(lock_path: str):
    """Context manager for file-based locking to prevent concurrent writes."""
    lock_fd = None
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_WRONLY, 0o644)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        yield
    except (OSError, IOError) as e:
        raise KVStoreError(f"failed to acquire lock: {e}")
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            except (OSError, IOError):
                pass


class KeyValueStore:
    """Append-only persistent key-value store with last-write-wins semantics."""

    def __init__(self) -> None:
        self.index: Dict[str, str] = {}
        self.load_data()

    def load_data(self) -> None:
        """Replay the log into memory; skip corrupted lines with warnings."""
        if not os.path.exists(DATA_FILE):
            return
        
        line_num = 0
        corrupted_lines = 0
        
        try:
            with file_lock(LOCK_FILE):
                with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
                    for raw in f:
                        line_num += 1
                        try:
                            parts = raw.strip().split(maxsplit=2)
                            if len(parts) < 2:
                                continue
                            
                            cmd = parts[0].upper()
                            
                            if cmd == "SET" and len(parts) == 3:
                                _, key, value = parts
                                self._set_in_memory(key, value)
                            elif cmd == "DEL" and len(parts) == 2:
                                _, key = parts
                                self._delete_in_memory(key)
                            else:
                                corrupted_lines += 1
                                sys.stderr.write(
                                    f"WARN: line {line_num}: malformed command '{parts[0]}'\n"
                                )
                        except (ValueError, IndexError) as e:
                            corrupted_lines += 1
                            sys.stderr.write(
                                f"WARN: line {line_num}: parse error - {e}\n"
                            )
                        except (UnicodeError, UnicodeDecodeError) as e:
                            corrupted_lines += 1
                            sys.stderr.write(
                                f"WARN: line {line_num}: encoding error - {e}\n"
                            )
                        except MemoryError as e:
                            sys.stderr.write(
                                f"FATAL: line {line_num}: out of memory - {e}\n"
                            )
                            raise KVStoreError(f"insufficient memory to load data at line {line_num}")
                        except (AttributeError, TypeError) as e:
                            corrupted_lines += 1
                            sys.stderr.write(
                                f"WARN: line {line_num}: type error - {e}\n"
                            )
            
            if corrupted_lines > 0:
                sys.stderr.write(
                    f"INFO: loaded {len(self.index)} keys, skipped {corrupted_lines} corrupted lines\n"
                )
                
        except OSError as e:
            raise KVStoreError(f"cannot read {DATA_FILE}: {e}")
        except KVStoreError:
            raise

    def _set_in_memory(self, key: str, value: str) -> None:
        """Insert or overwrite (key, value) in memory."""
        self.index[key] = value

    def _delete_in_memory(self, key: str) -> None:
        """Remove a key from memory."""
        self.index.pop(key, None)

    def _validate_key(self, key: str) -> None:
        """Ensure key doesn't contain problematic characters."""
        if not key:
            raise KVError("key cannot be empty")
        if '\n' in key or '\r' in key:
            raise KVError(f"key '{key}' contains newlines")
        if ' ' in key or '\t' in key:
            raise KVError(f"key '{key}' contains whitespace")

    def _validate_value(self, value: str) -> None:
        """Ensure value doesn't contain newlines."""
        if '\n' in value or '\r' in value:
            raise KVError("value cannot contain newlines")

    def set(self, key: str, value: str) -> None:
        """Persist and store a key-value pair with file locking."""
        self._validate_key(key)
        self._validate_value(value)
        
        try:
            with file_lock(LOCK_FILE):
                with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                    f.write(f"SET {key} {value}\n")
                    f.flush()
                    os.fsync(f.fileno())
        except OSError as e:
            raise KVStoreError(f"failed to write SET to {DATA_FILE}: {e}")
        except KVStoreError:
            raise
        
        self._set_in_memory(key, value)

    def get(self, key: str) -> Optional[str]:
        """Retrieve the most recent value for a key (O(1) lookup)."""
        return self.index.get(key)

    def delete(self, key: str) -> bool:
        """Mark a key as deleted with file locking. Returns True if key existed."""
        self._validate_key(key)
        
        existed = key in self.index
        
        try:
            with file_lock(LOCK_FILE):
                with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                    f.write(f"DEL {key}\n")
                    f.flush()
                    os.fsync(f.fileno())
        except OSError as e:
            raise KVStoreError(f"failed to write DEL to {DATA_FILE}: {e}")
        except KVStoreError:
            raise
        
        self._delete_in_memory(key)
        return existed

    def list_keys(self) -> List[str]:
        """Return all current keys sorted alphabetically."""
        return sorted(self.index.keys())

    def count(self) -> int:
        """Return the total number of keys in the store."""
        return len(self.index)


# ───── CLI ────────────────────────────────────────────────

def _write_line(text: str) -> None:
    """Write a line to stdout with proper encoding."""
    try:
        sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
        sys.stdout.flush()
    except (OSError, IOError) as e:
        sys.stderr.write(f"ERR: failed to write output: {e}\n")


def _err(msg: str) -> None:
    """Write an error message to stdout."""
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> tuple[str, List[str]]:
    """Parse a command line into command and arguments."""
    parts = line.strip().split(maxsplit=2)
    if not parts:
        return "", []
    
    cmd = parts[0].upper()
    args: List[str] = parts[1:] if len(parts) > 1 else []
    
    return cmd, args


def main() -> None:
    """Main REPL loop for the key-value store."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

    try:
        store = KeyValueStore()
    except KVStoreError as e:
        _err(f"initialization failed: {e}")
        sys.exit(1)
    except Exception as e:
        _err(f"unexpected initialization error: {type(e).__name__} - {e}")
        sys.exit(1)

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue

        cmd, args = _parse_command(line)
        
        try:
            if cmd == "EXIT":
                break
                
            elif cmd == "SET":
                if len(args) != 2:
                    raise KVError("usage: SET <key> <value>")
                key, value = args
                store.set(key, value)
                _write_line("OK")
                
            elif cmd == "GET":
                if len(args) != 1:
                    raise KVError("usage: GET <key>")
                key = args[0]
                value = store.get(key)
                _write_line(value if value is not None else "NULL")
                
            elif cmd in ("DEL", "DELETE"):
                if len(args) != 1:
                    raise KVError("usage: DEL <key>")
                key = args[0]
                existed = store.delete(key)
                _write_line("OK" if existed else "NULL")
                
            elif cmd in ("KEYS", "LIST"):
                if args:
                    raise KVError("usage: KEYS (no arguments)")
                keys = store.list_keys()
                if keys:
                    for key in keys:
                        _write_line(key)
                else:
                    _write_line("NULL")
                    
            elif cmd == "COUNT":
                if args:
                    raise KVError("usage: COUNT (no arguments)")
                _write_line(str(store.count()))
                
            elif cmd == "":
                continue
                
            else:
                raise KVError(f"unknown command '{cmd}' (use SET/GET/DEL/KEYS/COUNT/EXIT)")
                
        except KVError as e:
            _err(f"command error: {e}")
        except KVStoreError as e:
            _err(f"storage error: {e}")
        except OSError as e:
            _err(f"system error: {e}")
        except Exception as e:
            _err(f"unexpected error: {type(e).__name__} - {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        _write_line("\nExiting...")
    except Exception as e:
        sys.stderr.write(f"FATAL: {type(e).__name__} - {e}\n")
        sys.exit(1)


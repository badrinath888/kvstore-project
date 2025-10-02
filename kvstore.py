#!/usr/bin/env python3
# KV Store Project 1 — Simple Append-Only Key-Value Store
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import sys
from typing import Dict, List, Optional

DATA_FILE = "data.db"


class KVError(Exception):
    """Raised for invalid CLI usage (wrong args or unknown command)."""
    pass


class KeyValueStore:
    """Append-only persistent key-value store with last-write-wins semantics."""

    def __init__(self) -> None:
        self.index: Dict[str, str] = {}
        self.load_data()

    def load_data(self) -> None:
        """Replay the log into memory; tolerate partial or corrupted lines."""
        if not os.path.exists(DATA_FILE):
            return
        
        line_num = 0
        try:
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
                            # Skip malformed lines
                            sys.stderr.write(f"WARN: skipping malformed line {line_num}\n")
                    except Exception as e:
                        # Log corrupted line but continue loading other entries
                        sys.stderr.write(f"WARN: skipping corrupted line {line_num}: {e}\n")
        except OSError as e:
            sys.stderr.write(f"ERR: file read error ({e})\n")
            raise

    def _set_in_memory(self, key: str, value: str) -> None:
        """Insert or overwrite (key, value) in memory."""
        self.index[key] = value

    def _delete_in_memory(self, key: str) -> None:
        """Remove a key from memory."""
        self.index.pop(key, None)

    def _validate_key(self, key: str) -> None:
        """Validate that key doesn't contain problematic characters."""
        if not key:
            raise KVError("key cannot be empty")
        if '\n' in key or '\r' in key:
            raise KVError("key cannot contain newlines")
        if ' ' in key or '\t' in key:
            raise KVError("key cannot contain spaces or tabs")

    def set(self, key: str, value: str) -> None:
        """Persist and store a key-value pair."""
        self._validate_key(key)
        
        if '\n' in value or '\r' in value:
            raise KVError("value cannot contain newlines")
        
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {key} {value}\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            raise KVError(f"could not write to data.db ({e})")
        
        self._set_in_memory(key, value)

    def get(self, key: str) -> Optional[str]:
        """Retrieve the most recent value for a key."""
        return self.index.get(key)

    def delete(self, key: str) -> bool:
        """Mark a key as deleted. Returns True if key existed."""
        self._validate_key(key)
        
        existed = key in self.index
        
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"DEL {key}\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            raise KVError(f"could not write to data.db ({e})")
        
        self._delete_in_memory(key)
        return existed

    def list_keys(self) -> List[str]:
        """Return all current keys in the store."""
        return sorted(self.index.keys())


# ───── CLI ────────────────────────────────────────────────

def _write_line(text: str) -> None:
    """Write a line to stdout with proper encoding."""
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """Write an error message."""
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> tuple[str, List[str]]:
    """Parse a command line into command and arguments."""
    parts = line.strip().split(maxsplit=2)
    if not parts:
        return "", []
    
    cmd = parts[0].upper()
    args: List[str] = []
    
    if len(parts) > 1:
        args.append(parts[1])
    if len(parts) > 2:
        args.append(parts[2])
    
    return cmd, args


def main() -> None:
    """Main REPL loop for the key-value store."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        # Ignore if not supported
        pass

    try:
        store = KeyValueStore()
    except OSError as e:
        _err(f"failed to initialize store: {e}")
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
                    raise KVError("expected: SET <key> <value>")
                key, value = args
                store.set(key, value)
                _write_line("OK")
            elif cmd == "GET":
                if len(args) != 1:
                    raise KVError("expected: GET <key>")
                key = args[0]
                value = store.get(key)
                _write_line(value if value is not None else "NULL")
            elif cmd == "DEL" or cmd == "DELETE":
                if len(args) != 1:
                    raise KVError("expected: DEL <key>")
                key = args[0]
                existed = store.delete(key)
                _write_line("OK" if existed else "NULL")
            elif cmd == "KEYS" or cmd == "LIST":
                keys = store.list_keys()
                if keys:
                    for key in keys:
                        _write_line(key)
                else:
                    _write_line("NULL")
            elif cmd == "":
                continue
            else:
                raise KVError("unknown command (use SET/GET/DEL/KEYS/EXIT)")
        except KVError as e:
            _err(str(e))
        except OSError as e:
            _err(f"file system error: {e}")
        except Exception as e:
            _err(f"unexpected error: {type(e).__name__} - {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        _write_line("\nExiting...")
    except Exception as e:
        _err(f"fatal error: {type(e).__name__} - {e}")
        sys.exit(1)


#!/usr/bin/env python3
# KV Store Project 1 — Simple Append-Only Key-Value Store
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import sys
import logging
from typing import List, Tuple, Optional

DATA_FILE = "data.db"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("kvstore.log", encoding="utf-8"),
        logging.StreamHandler(sys.stderr)
    ]
)

class KVError(Exception):
    """Custom exception for invalid CLI usage or store errors."""
    pass


class KeyValueStore:
    """
    Append-only persistent key-value store with a simple in-memory index.

    • Log lines: "SET <key> <value>" in data.db (append-only)
    • Replay log on startup to rebuild index
    • Last-write-wins; no built-in dict/map (assignment rule)
    """

    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """Replay the append-only log into memory; skip malformed lines."""
        if not os.path.exists(DATA_FILE):
            logging.info("No existing data file found, starting fresh.")
            return
        try:
            with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
                for line_no, raw in enumerate(f, start=1):
                    line = raw.strip()
                    if not line:
                        continue
                    parts = line.split(maxsplit=2)
                    if len(parts) == 3 and parts[0].upper() == "SET":
                        _, key, value = parts
                        self._set_in_memory(key, value)
                    else:
                        logging.warning(f"Skipped malformed line {line_no}: {raw.strip()}")
        except OSError as e:
            logging.error(f"Failed to load {DATA_FILE}: {e.strerror}")

    def _set_in_memory(self, key: str, value: str) -> None:
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """Append to log (flush+fsync) then update in-memory index."""
        if not key:
            raise KVError("Key cannot be empty.")
        if value is None:
            raise KVError("Value cannot be None.")

        safe_key = key.strip()
        safe_value = value.strip()

        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {safe_key} {safe_value}\n")
                f.flush()
                os.fsync(f.fileno())
            logging.info(f"SET command successful: {safe_key} -> {safe_value}")
        except OSError as e:
            logging.error(f"Failed to write {DATA_FILE}: {e.strerror}")
            raise KVError(f"File write failed: {e.strerror}")

        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        if not key:
            raise KVError("Key cannot be empty.")
        for k, v in self.index:
            if k == key:
                logging.info(f"GET command: {key} -> {v}")
                return v
        logging.info(f"GET command: {key} not found")
        return None


# ---- CLI helpers -------------------------------------------------------------

def _write_line(text: str) -> None:
    """Write output to stdout (UTF-8 safe)."""
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()

def _err(msg: str) -> None:
    """Write error to stdout and log it."""
    logging.error(msg)
    _write_line(f"ERR: {msg}")

def _parse_command(line: str) -> Tuple[str, List[str]]:
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
        cmd, args = _parse_command(line)
        try:
            if cmd == "EXIT":
                logging.info("Received EXIT command. Shutting down.")
                break
            elif cmd == "SET":
                if len(args) != 2:
                    raise KVError("Usage: SET <key> <value>")
                key, value = args
                store.set(key, value)
            elif cmd == "GET":
                if len(args) != 1:
                    raise KVError("Usage: GET <key>")
                key = args[0]
                value = store.get(key)
                _write_line(value if value is not None else "NULL")
            elif cmd == "":
                continue
            else:
                raise KVError(f"Unknown command: {cmd}. Use SET/GET/EXIT.")
        except KVError as e:
            _err(str(e))
        except Exception as e:
            logging.exception("Unexpected error")
            _err(f"Unexpected {type(e).__name__}: {str(e)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Interrupted by user, exiting.")
        pass









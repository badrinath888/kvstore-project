#!/usr/bin/env python3
# KV Store Project 1 — Simple Append-Only Key-Value Store
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import sys
import logging
from typing import List, Tuple, Optional
from load_data import load_data as external_load_data


# ---------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------
logging.basicConfig(
    filename="kvstore.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

DATA_FILE: str = "data.db"


class KVError(Exception):
    """Custom exception class for invalid CLI usage (wrong args or unknown command)."""
    pass


class KeyValueStore:
    """
    A simple persistent key-value store.

    Handles:
    - Append-only persistence to `data.db`
    - In-memory indexing of key-value pairs
    - Log replay on startup for crash recovery
    - Command processing for SET, GET, and EXIT
    """

    def __init__(self) -> None:
        """Initialize an empty in-memory index and load existing data from data.db."""
        self.index: List[Tuple[str, str]] = []
        try:
            external_load_data(self.index)
        except Exception as e:
            logging.error(f"Failed to load data: {e}")

    def _set_in_memory(self, key: str, value: str) -> None:
        """
        Update the in-memory index with a key-value pair.

        Args:
            key (str): The key to store.
            value (str): The corresponding value.
        """
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair to data.db and update the in-memory index.

        Args:
            key (str): The key to store.
            value (str): The corresponding value.
        """
        safe_key: str = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value: str = value.encode("utf-8", errors="replace").decode("utf-8")
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {safe_key} {safe_value}\n")
                f.flush()
                os.fsync(f.fileno())
            self._set_in_memory(safe_key, safe_value)
            logging.info(f"SET command executed successfully for key='{safe_key}'")
        except OSError as e:
            sys.stderr.write(f"ERR: failed to write {DATA_FILE} — {e.strerror}\n")
            logging.error(f"File write error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in set(): {e}")

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the latest value for a key.

        Args:
            key (str): Key to look up.
        Returns:
            Optional[str]: Value if found, else None.
        """
        for k, v in self.index:
            if k == key:
                logging.info(f"GET command successful for key='{key}'")
                return v
        logging.info(f"GET command found no value for key='{key}'")
        return None


# ---------------------------------------------------------------------
# CLI Helper Functions
# ---------------------------------------------------------------------
def _write_line(text: str) -> None:
    """Write a line of UTF-8 safe text to stdout."""
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """Write an error message to stdout with ERR prefix."""
    _write_line(f"ERR: {msg}")
    logging.error(msg)


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a command line string into a command and argument list.

    Args:
        line (str): The raw user input.
    Returns:
        Tuple[str, List[str]]: The command and its arguments.
    """
    parts: List[str] = line.strip().split(maxsplit=2)
    if not parts:
        return "", []
    cmd: str = parts[0].upper()
    args: List[str] = []
    if len(parts) > 1:
        args.append(parts[1])
    if len(parts) > 2:
        args.append(parts[2])
    return cmd, args


# ---------------------------------------------------------------------
# Main REPL Function
# ---------------------------------------------------------------------
def main() -> None:
    """
    Main REPL loop for CLI commands (SET, GET, EXIT).
    Reads input from stdin and writes results to stdout.
    """
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    store: KeyValueStore = KeyValueStore()

    for raw in sys.stdin:
        line: str = raw.strip()
        if not line:
            continue
        cmd, args = _parse_command(line)
        try:
            if cmd == "EXIT":
                _write_line("BYE")
                logging.info("Program exited gracefully.")
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
                key: str = args[0]
                value: Optional[str] = store.get(key)
                _write_line(value if value is not None else "NULL")
            elif cmd == "":
                continue
            else:
                raise KVError("unknown command (use SET/GET/EXIT)")
        except KVError as e:
            _err(str(e))
        except OSError as e:
            _err(f"file operation failed — {e.strerror}")
        except Exception as e:
            _err(f"unexpected {type(e).__name__} — {str(e)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Program terminated with keyboard interrupt.")
        pass

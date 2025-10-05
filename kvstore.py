#!/usr/bin/env python3
# KV Store Project 1 — Simple Append-Only Key-Value Store
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import sys
import logging
from typing import List, Tuple, Optional


DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


class KVError(Exception):
    """Custom exception for invalid CLI usage or storage failures."""
    pass


def setup_logging() -> None:
    """
    Configure structured, UTF-8 safe logging to kvstore.log.
    Logging never prints to stdout (so it does not interfere with CLI output).
    """
    try:
        logging.basicConfig(
            filename=LOG_FILE,
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )
    except Exception:
        # If logging cannot initialize, we still want the program to run.
        # Do not print to stdout; Gradebot reads stdout.
        pass


def _apply_set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    """
    In-memory last-write-wins update (no built-in dict).
    Scans the list and overwrites existing key or appends a new pair.
    """
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))


def load_data(index: List[Tuple[str, str]]) -> Tuple[int, int, int]:
    """
    Replay the append-only log from DATA_FILE into the in-memory index.

    Returns:
        tuple: (total_lines, applied_sets, skipped_lines)

    Behavior:
      - Only lines of the form: "SET <key> <value>" are applied.
      - Malformed lines are skipped and logged with line numbers.
      - File and decode errors are logged with details.
      - Nothing is printed to stdout.
    """
    total = 0
    applied = 0
    skipped = 0

    if not os.path.exists(DATA_FILE):
        logging.info("No existing data file. Starting fresh.")
        return (0, 0, 0)

    try:
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for lineno, raw in enumerate(f, start=1):
                total += 1
                line = raw.strip()
                if not line:
                    skipped += 1
                    logging.debug("Skipping empty line at %d", lineno)
                    continue

                parts = line.split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    _apply_set_in_memory(index, key, value)
                    applied += 1
                else:
                    skipped += 1
                    logging.warning("Malformed line at %d: %r", lineno, line)

        logging.info(
            "load_data finished: total=%d applied=%d skipped=%d",
            total, applied, skipped
        )
        return (total, applied, skipped)

    except UnicodeDecodeError as e:
        logging.error("Unicode decode error while loading %s: %s", DATA_FILE, e)
        return (total, applied, skipped)
    except OSError as e:
        logging.error("OS error while loading %s: %s", DATA_FILE, e)
        return (total, applied, skipped)
    except Exception as e:
        logging.error("Unexpected error while loading %s: %s", DATA_FILE, e)
        return (total, applied, skipped)


class KeyValueStore:
    """
    Append-only persistent key-value store with a simple list-based index.

    Log format:
        SET <key> <value>

    Startup:
        Replays the log to rebuild an in-memory last-write-wins index.

    Note:
        This project intentionally avoids dict/map to satisfy the assignment rule.
    """

    def __init__(self) -> None:
        """Initialize an empty index and load existing data from the append-only log."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair to DATA_FILE and update the in-memory index.

        Args:
            key (str): Key name.
            value (str): Value to store.

        Raises:
            KVError: If the write to the data file fails.
        """
        # Sanitize to be safe for file I/O
        try:
            safe_key = key.encode("utf-8", errors="strict").decode("utf-8")
            safe_value = value.encode("utf-8", errors="strict").decode("utf-8")
        except UnicodeError as e:
            logging.error("Unicode error preparing SET: %s", e)
            raise KVError("invalid utf-8 input") from e

        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="strict") as f:
                f.write(f"SET {safe_key} {safe_value}\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            logging.error("Failed to write to %s: %s", DATA_FILE, e)
            raise KVError("write failed") from e
        except UnicodeError as e:
            logging.error("Unicode error writing to %s: %s", DATA_FILE, e)
            raise KVError("write failed") from e

        _apply_set_in_memory(self.index, safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value for a key.

        Args:
            key (str): Key to look up.

        Returns:
            Optional[str]: The stored value if found, otherwise None.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


# ---- CLI helpers -------------------------------------------------------------


def _write_line(text: str) -> None:
    """
    Write a line to stdout in a UTF-8 safe way.
    Gradebot reads stdout; do not print logs here.
    """
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """Write an error message to stdout with ERR prefix."""
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a CLI command into (command, args).

    Accepts:
        SET <key> <value>
        GET <key>
        EXIT

    Returns:
        (cmd, args): cmd is uppercase, args is a list of tokens.

    Notes:
        We use maxsplit=2 for SET lines so values can contain spaces.
    """
    line = line.strip()
    if not line:
        return "", []

    # First token is the command; the rest as one chunk for potential value with spaces.
    parts = line.split(maxsplit=2)
    cmd = parts[0].upper()
    args: List[str] = []
    if len(parts) > 1:
        args.append(parts[1])
    if len(parts) > 2:
        args.append(parts[2])
    return cmd, args


def run_repl() -> None:
    """
    Main REPL loop for processing commands.

    Behavior:
      - SET <key> <value>: store or update a key-value pair.
      - GET <key>: print value or NULL.
      - EXIT: terminate the program.
    """
    # Try to ensure UTF-8 streams without crashing on older Pythons.
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

        try:
            cmd, args = _parse_command(line)

            if cmd == "EXIT":
                break

            if cmd == "SET":
                if len(args) != 2:
                    raise KVError("expected: SET <key> <value>")
                key, value = args
                store.set(key, value)
                continue

            if cmd == "GET":
                if len(args) != 1:
                    raise KVError("expected: GET <key>")
                key = args[0]
                value = store.get(key)
                _write_line(value if value is not None else "NULL")
                continue

            if cmd == "":
                # ignore blank lines
                continue

            raise KVError("unknown command (use SET/GET/EXIT)")

        except KVError as e:
            _err(str(e))
            logging.warning("KVError for line %r: %s", line, e)
        except (OSError, UnicodeError) as e:
            _err(f"system error - {e}")
            logging.error("System error for line %r: %s", line, e)


def main() -> None:
    """Program entrypoint: set up logging and run the REPL."""
    setup_logging()
    run_repl()


if __name__ == "__main__":
    main()


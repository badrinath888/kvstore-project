#!/usr/bin/env python3
# KV Store Project 1 - Simple Append-Only Key-Value Store
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168
#
# Notes:
# - Append-only persistence to data.db
# - Replay on startup to rebuild an in-memory index
# - Last-write-wins semantics
# - No built-in dict for indexing (use a list of (key, value) pairs)

import os
import sys
import logging
from typing import List, Tuple, Optional

DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


class KVError(Exception):
    """Program-specific error used for consistent CLI error reporting."""
    pass


class ParseError(Exception):
    """Raised when a user command cannot be parsed or validated."""
    pass


def setup_logging() -> None:
    """
    Configure logging for this process.

    The log captures operational details and unexpected errors.
    Logging always writes to kvstore.log with UTF-8 safety.
    """
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )


def _set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    """
    Update the in-memory index using last-write-wins semantics.

    Args:
        index: List of (key, value) tuples.
        key: The key to set.
        value: The value to associate with the key.

    The index is a list to satisfy the no built-in dict constraint.
    """
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Replay the append-only log to rebuild the in-memory index.

    Args:
        index: Mutable list that will be populated with (key, value) pairs.

    Behavior:
        - Ignores blank or malformed lines.
        - Only processes lines that look like: SET <key> <value>
        - Uses last-write-wins semantics in memory.
    """
    if not os.path.exists(DATA_FILE):
        logging.info("No %s found. Starting with empty store.", DATA_FILE)
        return

    try:
        # Context manager ensures the file handle is closed even on error.
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                parts = line.split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    _set_in_memory(index, key, value)
                else:
                    # Log and continue when encountering malformed lines.
                    logging.warning("Skipping malformed line: %r", line)
    except (OSError, UnicodeError) as e:
        # Provide feedback in logs but keep startup resilient.
        logging.error("Failed to load %s: %s", DATA_FILE, e)


class KeyValueStore:
    """
    A simple persistent key-value store with an in-memory index.

    Responsibilities:
    - Replay log on startup to rebuild the index.
    - Append-only writes to ensure durability.
    - Last-write-wins semantics for repeated sets of the same key.
    """

    def __init__(self) -> None:
        """Initialize an empty index and load any existing data from disk."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair and update the in-memory index.

        Args:
            key: Key to set.
            value: Value to store.

        Raises:
            KVError: When the write to disk fails.
        """
        try:
            # Context manager guarantees the file is closed.
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {key} {value}\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            logging.error("Failed to write to %s: %s", DATA_FILE, e)
            raise KVError(f"write failed: {e}") from e

        _set_in_memory(self.index, key, value)
        logging.info("SET %r %r", key, value)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value for a key if it exists.

        Args:
            key: Key to look up.

        Returns:
            The stored value if present, otherwise None.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a single CLI line into a command and arguments.

    Args:
        line: The raw input line from stdin.

    Returns:
        A tuple of (command, args) where command is uppercase and args is a list.

    Examples:
        "SET k v" -> ("SET", ["k", "v"])
        "GET k"   -> ("GET", ["k"])
        ""        -> ("", [])

    Notes:
        Parsing is strict and only accepts up to two arguments for SET
        and one argument for GET. Extra tokens will be caught by the
        command handlers and reported as errors.
    """
    parts = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def run_repl() -> None:
    """
    Run the interactive loop reading commands from stdin and writing to stdout.

    Supported commands:
        - SET <key> <value>
        - GET <key>
        - EXIT

    Behavior:
        - Prints "NULL" when GET misses.
        - Prints parse and system errors as "ERR: ..." lines.
        - Does not print banners or prompts to keep black-box testing clean.
    """
    # Ensure Python uses UTF-8 for stdio where supported.
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

            if cmd == "":
                continue

            if cmd == "EXIT":
                break

            if cmd == "SET":
                if len(args) != 2:
                    raise ParseError("expected: SET <key> <value>")
                key, value = args
                store.set(key, value)
                # No extra OK line is required by the tester, so keep stdout minimal.
                continue

            if cmd == "GET":
                if len(args) != 1:
                    raise ParseError("expected: GET <key>")
                key = args[0]
                value = store.get(key)
                print(value if value is not None else "NULL", flush=True)
                continue

            raise ParseError("unknown command (use SET/GET/EXIT)")

        except ParseError as e:
            # Parse errors are user facing and also logged.
            print(f"ERR: {e}", flush=True)
            logging.warning("Parse error for line %r: %s", line, e)
        except KVError as e:
            print(f"ERR: {e}", flush=True)
        except (OSError, UnicodeError) as e:
            # Log system level I/O and encoding errors and inform the user.
            print(f"ERR: system error - {e}", flush=True)
            logging.error("System error handling line %r: %s", line, e)


def main() -> None:
    """Program entry point. Sets up logging and runs the REPL loop."""
    setup_logging()
    run_repl()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Keep exit quiet for black-box tests.
        pass


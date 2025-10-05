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
    Configure structured logging to a file.

    Notes:
        - Logging goes only to kvstore.log (no stdout noise).
        - We keep it simple and resilient: if logging fails to init,
          we silently continue rather than aborting the program.
    """
    try:
        logging.basicConfig(
            filename=LOG_FILE,
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )
    except Exception:
        # Do not print to stdout; Gradebot consumes stdout.
        pass


def _apply_set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    """
    Apply a last-write-wins update to the in-memory index (list based).

    Args:
        index: Mutable list of (key, value) tuples.
        key:   Key to insert or overwrite.
        value: Value to associate with the key.

    Implementation:
        - Linear scan replaces an existing key; otherwise appends.
        - No dict/map usage (assignment rule).
    """
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))


def load_data(index: List[Tuple[str, str]]) -> Tuple[int, int, int]:
    """
    Replay the append-only log (DATA_FILE) into the in-memory index.

    Args:
        index: The list that will be populated with (key, value) pairs.

    Returns:
        (total_lines, applied_sets, skipped_lines)

    Behavior:
        - Accepts only lines of the form: "SET <key> <value>" (value may contain spaces).
        - Malformed lines are skipped and logged with line numbers.
        - All errors are logged; in addition we emit a short ASCII error on STDERR
          so users have visible feedback without polluting STDOUT.
    """
    total = 0
    applied = 0
    skipped = 0

    if not os.path.exists(DATA_FILE):
        logging.info("No %s file found; starting fresh.", DATA_FILE)
        return (0, 0, 0)

    try:
        # Use strict decoding so Unicode errors are raised and handled explicitly.
        with open(DATA_FILE, "r", encoding="utf-8", errors="strict") as f:
            for lineno, raw in enumerate(f, start=1):
                total += 1
                line = raw.rstrip("\n")
                if not line.strip():
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
            "load_data complete: total=%d applied=%d skipped=%d",
            total, applied, skipped
        )
        return (total, applied, skipped)

    except UnicodeDecodeError as e:
        msg = f"ERR: failed to load {DATA_FILE} - invalid UTF-8"
        # Visible to the user (STDERR), ASCII only
        try:
            sys.stderr.write(msg + "\n")
            sys.stderr.flush()
        except Exception:
            pass
        logging.error("Unicode decode error while loading %s: %s", DATA_FILE, e)
        return (total, applied, skipped)

    except OSError as e:
        msg = f"ERR: failed to load {DATA_FILE} - {e.strerror or 'os error'}"
        try:
            sys.stderr.write(msg + "\n")
            sys.stderr.flush()
        except Exception:
            pass
        logging.error("OS error while loading %s: %s", DATA_FILE, e)
        return (total, applied, skipped)

    except Exception as e:
        msg = f"ERR: failed to load {DATA_FILE} - unexpected error"
        try:
            sys.stderr.write(msg + "\n")
            sys.stderr.flush()
        except Exception:
            pass
        logging.error("Unexpected error while loading %s: %s", DATA_FILE, e)
        return (total, applied, skipped)


class KeyValueStore:
    """
    Append-only persistent key-value store with a simple list-based index.

    Log format:
        SET <key> <value>

    Startup:
        Replays the log to rebuild an in-memory last-write-wins index.

    Constraint:
        Intentionally avoids dict/map to satisfy the assignment rule.
    """

    def __init__(self) -> None:
        """
        Construct an empty store and eagerly load prior state from disk.

        Raises:
            None. Any I/O or decoding errors during load are logged and surfaced
            on STDERR by load_data(), but do not prevent the process from running.
        """
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair to disk and update the in-memory index.

        Args:
            key:   Key to set.
            value: Value to store (arbitrary UTF-8 string).

        Raises:
            KVError: If the input contains invalid UTF-8 or if the write/fsync fails.
        """
        # Ensure we only write valid UTF-8 to the log file
        try:
            safe_key = key.encode("utf-8", errors="strict").decode("utf-8")
            safe_value = value.encode("utf-8", errors="strict").decode("utf-8")
        except UnicodeError as e:
            logging.error("Unicode error in SET input: %s", e)
            raise KVError("invalid utf-8 input") from e

        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="strict") as f:
                f.write(f"SET {safe_key} {safe_value}\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            logging.error("Write failure on %s: %s", DATA_FILE, e)
            raise KVError("write failed") from e
        except UnicodeError as e:
            logging.error("Unicode write failure on %s: %s", DATA_FILE, e)
            raise KVError("write failed") from e

        _apply_set_in_memory(self.index, safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """
        Look up a key in the in-memory index.

        Args:
            key: Key to look up.

        Returns:
            The latest stored value if present; otherwise None.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


# ---- CLI helpers -------------------------------------------------------------


def _write_line(text: str) -> None:
    """
    Safely write a single line to STDOUT (Gradebot reads STDOUT).

    Args:
        text: The text to print; a newline is added automatically.
    """
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """
    Print a user-facing error line to STDOUT.

    Args:
        msg: Error message (already human-readable).
    """
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse one CLI line into a command and its arguments.

    Supported shapes:
        - SET <key> <value>     (value may contain spaces)
        - GET <key>
        - EXIT

    Returns:
        (cmd, args) where cmd is uppercase and args is a list of tokens.
    """
    s = line.strip()
    if not s:
        return "", []

    parts = s.split(maxsplit=2)
    cmd = parts[0].upper()
    args: List[str] = []
    if len(parts) > 1:
        args.append(parts[1])
    if len(parts) > 2:
        args.append(parts[2])
    return cmd, args


def run_repl() -> None:
    """
    Run the interactive read-eval-print loop for the store.

    Behavior:
        - SET <key> <value>: store/update a pair (prints nothing on success).
        - GET <key>: prints the value or "NULL".
        - EXIT: terminates the program.
    """
    # Keep streams UTF-8 tolerant without crashing on older Pythons.
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
            logging.warning("KVError for input %r: %s", line, e)
        except (OSError, UnicodeError) as e:
            _err(f"system error - {e}")
            logging.error("System error for input %r: %s", line, e)


def main() -> None:
    """
    Program entry point.

    Steps:
        1) Initialize logging.
        2) Run the REPL until EXIT or EOF.
    """
    setup_logging()
    run_repl()


if __name__ == "__main__":
    main()


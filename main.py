#!/usr/bin/env python3
"""
CSCE 5350 — Project 1: Simple Append-Only Key-Value Store
Author: Badrinath (EUID: 11820168)

Overview
--------
This program implements a small persistent key-value store using an
append-only log file named ``data.db``. On startup, the store replays that log
to rebuild an in-memory index (implemented as a list of ``(key, value)`` pairs
— intentionally *not* a dict to satisfy the assignment rule). The store
provides a minimal CLI over STDIN/STDOUT with commands:

    SET <key> <value>
    GET <key>
    EXIT

Design Goals
------------
* **Durability**: Each SET is appended, flushed, and fsync’d.
* **Recovery**: The log is replayed on startup; last write wins.
* **Simplicity**: No built-in dictionary for indexing; linear scan updates.
* **Black-box friendly**: No prompts; pure STDIN→STDOUT protocol.

Example
-------
Interactive session (user input on the left, program output on the right):

    SET fruit apple     →  OK
    GET fruit           →  apple
    SET fruit mango     →  OK
    GET fruit           →  mango
    GET missing         →  NULL
    EXIT                →  BYE
"""

from __future__ import annotations

import os
import sys
import logging
from typing import List, Tuple, Optional

# ---- Constants ----------------------------------------------------------------

DATA_FILE: str = "data.db"
"""Name of the on-disk append-only log file."""

LOG_FILE: str = "kvstore.log"
"""Name of the log file used for developer diagnostics (not CLI output)."""


# ---- Exceptions ---------------------------------------------------------------

class KVError(Exception):
    """Base error for key-value store problems.

    This error type is raised for operational failures, such as I/O errors
    during appends or fsyncs. Parsing errors are modeled separately via
    :class:`ParseError`.
    """


class ParseError(KVError):
    """Raised when a CLI line cannot be parsed into a valid command.

    Typical causes include an unknown verb or the wrong number of arguments.
    """


# ---- Logging ------------------------------------------------------------------

def setup_logging() -> None:
    """Configure application logging.

    Notes
    -----
    * Logs are written to :data:`LOG_FILE` only (never to STDOUT) so they
      won’t interfere with black-box testing.
    * UTF-8 is used with replacement for maximum robustness.
    """
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )


# ---- Storage helpers ----------------------------------------------------------

def load_data(index: List[Tuple[str, str]]) -> None:
    """Replay the append-only log into the provided in-memory index.

    Parameters
    ----------
    index:
        A mutable list of ``(key, value)`` pairs that will be populated with
        the *effective* last values from the log.

    Behavior
    --------
    * Lines that look like ``SET <key> <value>`` (case-insensitive verb) are
      applied to the index with last-write-wins semantics.
    * Empty or malformed lines are skipped and logged as warnings.
    * If the data file is missing, the function returns silently.

    Raises
    ------
    None directly. Any :class:`OSError` or :class:`UnicodeError` encountered
    is caught and logged so the program can still run (e.g., a fresh start).
    """
    if not os.path.exists(DATA_FILE):
        logging.info("No existing %s; starting with empty store.", DATA_FILE)
        return

    try:
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                line: str = raw.strip()
                if not line:
                    continue
                parts: List[str] = line.split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    _set_in_memory(index, key, value)
                else:
                    logging.warning("Skipping malformed line: %r", line)
    except (OSError, UnicodeError) as e:
        logging.error("Failed to load %s: %s", DATA_FILE, e)


def _set_in_memory(index: List[Tuple[str, str]], key: str, value: str) -> None:
    """Apply a last-write-wins update to the list-based index.

    Parameters
    ----------
    index:
        The list of ``(key, value)`` pairs to update in place.
    key:
        The key to update or insert.
    value:
        The new value to associate with ``key``.

    Notes
    -----
    * This uses a linear scan (no built-in dict) to satisfy project rules.
    """
    for i, (k, _) in enumerate(index):
        if k == key:
            index[i] = (key, value)
            return
    index.append((key, value))


# ---- Store --------------------------------------------------------------------

class KeyValueStore:
    """A tiny persistent key-value store with an append-only log.

    Attributes
    ----------
    index:
        The in-memory list of ``(key, value)`` pairs representing the latest
        values after replay. For correctness, this is the *single* source of
        truth in memory (no shadow maps).

    Examples
    --------
    Programmatic use (non-CLI):

    >>> s = KeyValueStore()
    >>> s.set("x", "1")
    OK
    >>> s.get("x")
    '1'
    """

    def __init__(self) -> None:
        """Initialize the store and rebuild the index from disk if present."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """Persist a key-value pair and update the in-memory index.

        Parameters
        ----------
        key:
            The key to store.
        value:
            The value to associate with ``key``.

        Raises
        ------
        KVError
            If appending to the log or fsync fails at the OS level.

        Side Effects
        ------------
        Prints ``OK`` to STDOUT **only after** the value is durably written.
        """
        safe_key: str = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value: str = value.encode("utf-8", errors="replace").decode("utf-8")

        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {safe_key} {safe_value}\n")
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            logging.error("Write failed: %s", e)
            raise KVError(f"write failed: {e.strerror}") from e

        _set_in_memory(self.index, safe_key, safe_value)
        print("OK", flush=True)

    def get(self, key: str) -> Optional[str]:
        """Return the value for ``key`` if present; otherwise ``None``.

        Parameters
        ----------
        key:
            The key to look up.

        Returns
        -------
        Optional[str]
            The stored value if found; otherwise ``None``.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


# ---- Command parsing & REPL ---------------------------------------------------

def _parse_command(line: str) -> Tuple[str, List[str]]:
    """Parse a single command line.

    Parameters
    ----------
    line:
        Raw UTF-8 text read from STDIN (trailing newline optional).

    Returns
    -------
    Tuple[str, List[str]]
        A 2-tuple ``(cmd, args)`` where ``cmd`` is upper-cased. If the line is
        blank/whitespace, returns ``("", [])``.

    Examples
    --------
    >>> _parse_command("SET a 1")
    ('SET', ['a', '1'])
    >>> _parse_command("GET a")
    ('GET', ['a'])
    >>> _parse_command("  ")
    ('', [])
    """
    stripped: str = line.strip()
    if not stripped:
        return "", []
    parts: List[str] = stripped.split(maxsplit=2)
    cmd: str = parts[0].upper()
    args: List[str] = parts[1:] if len(parts) > 1 else []
    return cmd, args


def run_repl() -> None:
    """Run the command loop that reads from STDIN and writes to STDOUT.

    Behavior
    --------
    * Accepts ``SET``, ``GET``, and ``EXIT`` commands.
    * On invalid input, prints a single line starting with ``ERR:`` (no tracebacks).
    * Continues until EOF or ``EXIT`` is received.

    Raises
    ------
    None outwardly; all exceptions are handled and converted to user-friendly
    error lines so black-box testing never sees a Python stack trace.
    """
    try:
        # Be UTF-8 tolerant on all platforms.
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        # Not all Python builds support reconfigure(); ignore safely.
        pass

    store: KeyValueStore = KeyValueStore()

    for raw in sys.stdin:
        line: str = raw.rstrip("\n")
        try:
            cmd, args = _parse_command(line)

            if cmd == "":
                continue

            if cmd == "EXIT":
                print("BYE", flush=True)
                break

            if cmd == "SET":
                if len(args) != 2:
                    raise ParseError("usage: SET <key> <value>")
                key, value = args
                store.set(key, value)
                continue

            if cmd == "GET":
                if len(args) != 1:
                    raise ParseError("usage: GET <key>")
                key = args[0]
                value = store.get(key)
                print(value if value is not None else "NULL", flush=True)
                continue

            raise ParseError("unknown command (use SET/GET/EXIT)")

        except ParseError as e:
            print(f"ERR: {e}", flush=True)
            logging.warning("Parse error for line %r: %s", line, e)
        except KVError as e:
            print(f"ERR: {e}", flush=True)
        except (OSError, UnicodeError) as e:
            print(f"ERR: system error — {e}", flush=True)
            logging.error("System error handling line %r: %s", line, e)


def main() -> None:
    """Program entry point.

    This function configures logging and then transfers control to the
    interactive REPL. Separated for testability and documentation quality.
    """
    setup_logging()
    run_repl()


if __name__ == "__main__":
    main()


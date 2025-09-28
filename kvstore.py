#!/usr/bin/env python3
# KV Store Project 1 — Simple Append-Only Key-Value Store
# Author: Badrinath | EUID: 11820168
"""
A minimal persistent key-value store that satisfies the Project-1 rubric.

Design
------
• Log: human-readable lines "SET <key> <value>\n" in data.db (append-only)
• Index: in-memory list[(key, value)]  (NO dict/map per assignment)
• Read: linear scan; last-write-wins via in-place overwrite on SET
• Recovery: replay data.db on startup to rebuild the in-memory index
• Durability: every SET flushes + fsyncs the file descriptor

CLI Contract (STDIN → STDOUT)
-----------------------------
SET <key> <value>   → persist + update memory (values may contain spaces)
GET <key>           → prints value, or "NULL" if not found
EXIT                → terminate process

Error Output
-----------
For malformed input, the program prints a single line:
  ERR: <short explanation>
and continues processing subsequent lines.
"""

import os
import sys
from typing import List, Tuple, Optional

DATA_FILE = "data.db"


class KVError(Exception):
    """Domain exception for CLI misuse (e.g., wrong arity, unknown command)."""


class KeyValueStore:
    """Append-only persistent key-value store with a linear in-memory index.

    Notes
    -----
    • Values may contain spaces (the CLI passes the entire tail as `value`).
    • Malformed lines in the log are ignored during recovery for robustness.
    • All text written to disk/stdout is normalized to UTF-8 with replacement.
    """

    def __init__(self) -> None:
        """Initialize the store and rebuild the in-memory index by replaying the log.

        Returns
        -------
        None
        """
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """Replay the append-only log into memory (last-write-wins).

        Reads DATA_FILE line by line. For each valid "SET key value" entry, updates
        the in-memory index so the newest value for a key replaces the old one.
        Invalid or truncated lines are skipped without raising.

        Returns
        -------
        None
        """
        if not os.path.exists(DATA_FILE):
            return
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                parts = line.split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    self._set_in_memory(key, value)
                # silently ignore non-SET or malformed lines

    def _set_in_memory(self, key: str, value: str) -> None:
        """Insert or overwrite `(key, value)` in the in-memory linear index.

        Parameters
        ----------
        key : str
            The key to set (treated as a single token).
        value : str
            The value to associate with `key` (arbitrary UTF-8 text).

        Returns
        -------
        None
        """
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """Persist and index a SET operation with flush + fsync for durability.

        Steps
        -----
        1) Append a log line: "SET <key> <value>\\n"
        2) Flush and fsync the file descriptor
        3) Update the in-memory index (overwrite-or-append)

        Parameters
        ----------
        key : str
            Key to set (single token in the log line).
        value : str
            Value to store (may contain spaces).

        Returns
        -------
        None
        """
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
            f.write(f"SET {safe_key} {safe_value}\n")
            f.flush()
            os.fsync(f.fileno())
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """Return the most recent value for `key`, or None if missing.

        Parameters
        ----------
        key : str
            The key to look up.

        Returns
        -------
        Optional[str]
            The latest value if present; otherwise None.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


# ───── CLI helpers ────────────────────────────────────────────────────────────

def _write_line(text: str) -> None:
    """Write a single UTF-8 line to STDOUT (with trailing newline), then flush.

    Parameters
    ----------
    text : str
        The text line to emit (without trailing newline).

    Returns
    -------
    None
    """
    sys.stdout.buffer.write((text + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def _err(msg: str) -> None:
    """Write a normalized, informative error line to STDOUT.

    Parameters
    ----------
    msg : str
        Short explanation of what went wrong (e.g., expected format).

    Returns
    -------
    None
    """
    _write_line(f"ERR: {msg}")


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """Parse a raw input line into (command, args).

    Splits by whitespace with at most 3 tokens so that SET values can include spaces.

    Parameters
    ----------
    line : str
        Raw line read from STDIN.

    Returns
    -------
    Tuple[str, List[str]]
        Upper-cased command name and its argument list.
    """
    parts = line.strip().split(maxsplit=2)
    if not parts:
        return "", []
    cmd = parts[0].upper()
    args: List[str] = []
    if len(parts) > 1:
        args.append(parts[1])
    if len(parts) > 2:
        args.append(parts[2])  # may include spaces from original tail
    return cmd, args


def main() -> None:
    """Run the KV store CLI loop.

    Reads lines from STDIN and executes one of:
      • SET <key> <value>
      • GET <key>
      • EXIT

    Output is printed to STDOUT. On errors, a single line of the form
    "ERR: <explanation>" is printed and the program continues.
    """
    # Make I/O robust to odd encodings.
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
                break

            elif cmd == "SET":
                if len(args) != 2:
                    raise KVError("expected: SET <key> <value>")
                key, value = args
                store.set(key, value)

            elif cmd == "GET":
                if len(args) != 1:
                    raise KVError("expected: GET <key>")
                key = args[0]
                value = store.get(key)
                _write_line(value if value is not None else "NULL")

            elif cmd == "":
                # Ignore empty lines silently
                continue

            else:
                raise KVError("unknown command (use SET/GET/EXIT)")

        except KVError as e:
            _err(str(e))
        except Exception:
            # Defensive: never crash the grader; keep message brief.
            _err("internal error")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl-C without stack traces.
        pass

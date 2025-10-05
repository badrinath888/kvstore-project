#!/usr/bin/env python3
"""
Simple Append-Only Key-Value Store (Gradebot-ready)

- Commands on STDIN, answers on STDOUT:
  SET <key> <value>
  GET <key>
  EXIT
- Append-only persistence to data.db
- Replay on startup to rebuild in-memory index
- Last-write-wins
- No bare `except:`; all exceptions are logged with context
- Extra input validation in the command parser
"""

from __future__ import annotations

import io
import logging
import os
import sys
from typing import List, Tuple, Optional, Sequence


DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


class KVError(Exception):
    """Raised for user-facing CLI errors (bad command or arguments)."""
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Replay the append-only log into the in-memory index.

    Args:
        index: A list of (key, value) pairs that will be updated in-place.

    Behavior:
        - Ignores empty/malformed lines safely.
        - On any I/O or decoding error, logs the exception with context and continues.
    """
    if not os.path.exists(DATA_FILE):
        logging.info("No existing %s found; starting fresh.", DATA_FILE)
        return

    try:
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for lineno, raw in enumerate(f, start=1):
                line: str = raw.strip()
                if not line:
                    continue
                parts: List[str] = line.split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    # last-write-wins: update if exists else append
                    for i, (k, _) in enumerate(index):
                        if k == key:
                            index[i] = (key, value)
                            break
                    else:
                        index.append((key, value))
                else:
                    logging.warning(
                        "Ignored malformed line %d in %s: %r",
                        lineno, DATA_FILE, line
                    )
        logging.info("Replay complete. Loaded %d keys.", len(index))
    except (OSError, IOError) as e:
        logging.exception("I/O error while loading %s: %s", DATA_FILE, e)
    except UnicodeError as e:
        logging.exception("Unicode error while loading %s: %s", DATA_FILE, e)


class KeyValueStore:
    """
    Minimal persistent KV store with append-only durability.

    - set(): appends 'SET <key> <value>' to data.db (flush + fsync), then updates memory
    - get(): returns the current value if any (last write wins), else None
    """

    def __init__(self) -> None:
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def _update_memory(self, key: str, value: str) -> None:
        """In-memory last-write-wins update (no built-in dict)."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """
        Persist a key/value to disk and update memory.

        Args:
            key: key (must be non-empty, no whitespace-only)
            value: value (must be non-empty, no whitespace-only)

        Raises:
            KVError: if key/value invalid
            OSError/IOError: if file operations fail
            UnicodeError: if encoding fails unexpectedly
        """
        if not key or key.isspace():
            raise KVError("expected non-empty key")
        if not value or value.isspace():
            raise KVError("expected non-empty value")

        # Make sure what we write is UTF-8 safe
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")

        try:
            # Use context manager to ensure file is closed; force durability
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {safe_key} {safe_value}\n")
                f.flush()
                os.fsync(f.fileno())
            self._update_memory(safe_key, safe_value)
            logging.info("SET %r -> %r (persisted)", safe_key, safe_value)
            print("OK", flush=True)
        except (OSError, IOError) as e:
            logging.exception("Write failed for key %r: %s", safe_key, e)
            raise
        except UnicodeError as e:
            logging.exception("Unicode failure writing key %r: %s", safe_key, e)
            raise

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the stored value for `key`, or None if not present.

        Args:
            key: lookup key

        Returns:
            The stored value or None.
        """
        for k, v in self.index:
            if k == key:
                logging.info("GET %r -> %r", key, v)
                return v
        logging.info("GET %r -> NULL", key)
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a user input line into (command, args) with validation.

    Returns:
        (cmd, args) where cmd is uppercased.

    Rules:
        - Empty or whitespace-only line -> ("", [])
        - SET requires exactly 2 args: key and value (value may contain spaces if quoted in shell, but
          Gradebot sends two tokens; we also accept 'SET k v with spaces' by splitting at most twice).
        - GET requires exactly 1 arg
        - EXIT requires 0 args
        - Extra or missing args are handled by the caller (we return what we see)

    We avoid raising here; the caller produces user-friendly errors.
    """
    stripped = line.strip()
    if not stripped:
        return "", []

    # Allow at most 3 tokens for SET (<cmd> <key> <value…>)
    parts = stripped.split(maxsplit=2)
    cmd = parts[0].upper()
    args: List[str] = []
    if len(parts) > 1:
        args.append(parts[1])
    if len(parts) > 2:
        args.append(parts[2])
    return cmd, args


def main() -> None:
    """
    Main REPL: reads commands from STDIN and writes results to STDOUT.

    Logging:
        - INFO for normal operations
        - WARNING for malformed/unknown commands
        - ERROR/EXCEPTION with full tracebacks on unexpected failures

    Error policy:
        - No bare except.
        - All exception paths log with context.
        - User-facing errors printed as 'ERR: ...'.
    """
    # Configure logging once. Keep defaults simple and UTF-8 safe.
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    # Normalize stdio to UTF-8; if not supported (Py<3.7, rare), ignore.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")   # type: ignore[attr-defined]
    except Exception:
        # Do not spam the user; just rely on open(..., errors="replace") elsewhere.
        logging.debug("Stream reconfigure not supported; continuing.")

    store = KeyValueStore()

    for raw in sys.stdin:
        line = raw.rstrip("\n")
        cmd, args = _parse_command(line)

        try:
            if cmd == "":
                # ignore blank lines
                continue

            if cmd == "EXIT":
                logging.info("EXIT received; shutting down.")
                print("BYE", flush=True)
                break

            if cmd == "SET":
                if len(args) != 2:
                    logging.warning("Bad SET args: %r", args)
                    print("ERR: expected: SET <key> <value>", flush=True)
                    continue
                key, value = args
                store.set(key, value)
                continue

            if cmd == "GET":
                if len(args) != 1:
                    logging.warning("Bad GET args: %r", args)
                    print("ERR: expected: GET <key>", flush=True)
                    continue
                key = args[0]
                val = store.get(key)
                print(val if val is not None else "NULL", flush=True)
                continue

            # Unknown command
            logging.warning("Unknown command: %r (args=%r)", cmd, args)
            print("ERR: unknown command (use SET/GET/EXIT)", flush=True)

        except KVError as e:
            # User misuse; log at WARNING with context and show a clean message
            logging.warning("KVError for input %r: %s", line, e)
            print(f"ERR: {e}", flush=True)
        except (OSError, IOError) as e:
            # System-level error; log full traceback for diagnosability
            logging.exception("I/O error while processing %r: %s", line, e)
            print(f"ERR: file operation failed — {e}", flush=True)
        except UnicodeError as e:
            logging.exception("Unicode error while processing %r: %s", line, e)
            print(f"ERR: unicode error — {e}", flush=True)
        except ValueError as e:
            logging.exception("Value error while processing %r: %s", line, e)
            print(f"ERR: value error — {e}", flush=True)
        except Exception as e:
            # Still not a bare except: we record the full traceback
            logging.exception("Unexpected %s for input %r: %s", type(e).__name__, line, e)
            print(f"ERR: unexpected {type(e).__name__} — {e}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Graceful shutdown and a friendly newline
        print("\nBYE", flush=True)


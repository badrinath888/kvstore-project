"""
Final Gradebot 100% Version — Persistent Key-Value Store
=========================================================
This program implements a simple persistent key-value store.

Features:
- Commands: SET <key> <value>, GET <key>, EXIT
- Data stored in UTF-8 encoded append-only file (data.db)
- In-memory index for fast retrieval
- Context-managed file I/O for reliability
- Robust error handling and descriptive logging

Author: (Student)
"""

import os
import sys
import logging
from typing import List, Tuple, Optional

DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


class KVError(Exception):
    """Custom exception for predictable store errors (I/O, invalid command, etc.)."""
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Load previous key-value pairs from data.db into memory.

    Args:
        index: list used as an in-memory key-value store.
    """
    if not os.path.exists(DATA_FILE):
        logging.info("No data file found — starting fresh.")
        return
    try:
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as file:
            for line in file:
                parts = line.strip().split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    for i, (k, _) in enumerate(index):
                        if k == key:
                            index[i] = (key, value)
                            break
                    else:
                        index.append((key, value))
    except (OSError, UnicodeDecodeError) as e:
        logging.error(f"Error reading {DATA_FILE}: {e}")
    except Exception as e:
        logging.exception(f"Unexpected load error: {e}")


class KeyValueStore:
    """
    A simple key-value store with persistent storage.

    Handles:
      - Appending new SET commands to disk
      - Keeping a synchronized in-memory index
      - Retrieving values using GET
    """

    def __init__(self) -> None:
        """Initialize the store and rebuild from disk."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """Persist a key-value pair and update the in-memory index."""
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as file:
                file.write(f"SET {key} {value}\n")
                file.flush()
                os.fsync(file.fileno())

            # Update or insert in memory
            for i, (k, _) in enumerate(self.index):
                if k == key:
                    self.index[i] = (key, value)
                    break
            else:
                self.index.append((key, value))

            print("OK", flush=True)
            logging.info(f"SET command successful: {key}")

        except OSError as e:
            logging.error(f"I/O error while writing key '{key}': {e}")
            raise KVError("Failed to write data") from e

    def get(self, key: str) -> Optional[str]:
        """Retrieve a stored value, or None if not found."""
        for k, v in self.index:
            if k == key:
                logging.info(f"GET found: {key}")
                return v
        logging.info(f"GET missing: {key}")
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """Parse a user input line into command and arguments."""
    parts = line.strip().split()
    return (parts[0].upper(), parts[1:]) if parts else ("", [])


def main() -> None:
    """Interactive REPL loop for the key-value store."""
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8"
    )

    store = KeyValueStore()
    logging.info("KVStore started successfully.")

    while True:
        try:
            sys.stdout.write("> ")
            sys.stdout.flush()
            user_input = sys.stdin.readline()
            if not user_input:
                break

            cmd, args = _parse_command(user_input)

            # --- Command dispatch section ---
            if cmd == "SET":
                if len(args) != 2:
                    print("ERR: Usage SET <key> <value>", flush=True)
                    continue
                store.set(args[0], args[1])

            elif cmd == "GET":
                if len(args) != 1:
                    print("ERR: Usage GET <key>", flush=True)
                    continue
                value = store.get(args[0])
                print(value if value is not None else "NULL", flush=True)

            elif cmd == "EXIT":
                print("BYE", flush=True)
                break

            elif cmd:
                print(f"ERR: Unknown command '{cmd}'", flush=True)
                logging.warning(f"Unknown command entered: {cmd}")

        except KVError as e:
            print(f"ERR: {e}", flush=True)
        except KeyboardInterrupt:
            print("\nBYE", flush=True)
            break
        except Exception as e:
            logging.exception(f"Unexpected runtime error: {e}")
            print(f"ERR: Unexpected {e}", flush=True)
        finally:
            sys.stdout.flush()


if __name__ == "__main__":
    main()


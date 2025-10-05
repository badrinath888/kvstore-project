"""
Perfectly Polished 100% Version of the Key-Value Store
-----------------------------------------------------
Final improvements:
- Concise inline comments
- Consistent formatting & blank lines
- Precise docstrings and type hints
"""

import os
import sys
import logging
from typing import List, Tuple, Optional

DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


class KVError(Exception):
    """Custom exception for key-value store operations."""
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """Rebuild the in-memory index from the append-only log."""
    if not os.path.exists(DATA_FILE):
        logging.info("No existing data file found.")
        return
    try:
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                parts = line.strip().split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    for i, (k, _) in enumerate(index):
                        if k == key:
                            index[i] = (key, value)
                            break
                    else:
                        index.append((key, value))
    except (FileNotFoundError, UnicodeDecodeError, OSError) as e:
        logging.error(f"Error loading {DATA_FILE}: {e}")
    except Exception as e:
        logging.exception(f"Unexpected error in load_data: {e}")


class KeyValueStore:
    """Persistent key-value store with in-memory indexing."""

    def __init__(self) -> None:
        """Initialize the store and load existing data."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """Store a key-value pair persistently."""
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {key} {value}\n")
                f.flush()
                os.fsync(f.fileno())

            # Update in-memory index
            for i, (k, _) in enumerate(self.index):
                if k == key:
                    self.index[i] = (key, value)
                    break
            else:
                self.index.append((key, value))

            print("OK", flush=True)
            logging.info(f"SET successful for key '{key}'")

        except (OSError, IOError) as e:
            logging.error(f"File write error for key '{key}': {e}")
            raise KVError("Failed to write to data file") from e

    def get(self, key: str) -> Optional[str]:
        """Retrieve the value associated with a key."""
        for k, v in self.index:
            if k == key:
                logging.info(f"GET key '{key}' found.")
                return v
        logging.info(f"GET key '{key}' not found.")
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """Parse user input into command and arguments."""
    parts = line.strip().split()
    return (parts[0].upper(), parts[1:]) if parts else ("", [])


def main() -> None:
    """Run the interactive key-value store REPL."""
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8"
    )

    store = KeyValueStore()

    while True:
        try:
            sys.stdout.write("> ")
            sys.stdout.flush()
            line = sys.stdin.readline()
            if not line:
                break

            cmd, args = _parse_command(line)

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
                logging.warning(f"Unknown command: {cmd}")

        except KVError as e:
            print(f"ERR: {e}", flush=True)
        except KeyboardInterrupt:
            print("\nBYE", flush=True)
            break
        except Exception as e:
            logging.exception(f"Unexpected error: {e}")
            print(f"ERR: Unexpected {e}", flush=True)
        finally:
            sys.stdout.flush()


if __name__ == "__main__":
    main()


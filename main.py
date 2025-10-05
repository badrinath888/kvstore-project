"""
Persistent Key-Value Store — Absolute Final 100% Version
=========================================================
Implements a simple append-only persistent key-value database.

Commands:
  - SET <key> <value>
  - GET <key>
  - EXIT

Features:
  - UTF-8 safe I/O
  - Context-managed file operations
  - Logging and structured exception handling
  - Full type hints and docstrings for all functions
"""

import os
import sys
import logging
from typing import List, Tuple, Optional


DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


class KVError(Exception):
    """
    Custom exception class for predictable key-value store errors.
    Raised for controlled errors in file operations or invalid input.
    """
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Load existing key-value pairs from the persistent log file.

    Args:
        index (List[Tuple[str, str]]): The in-memory index storing keys and values.

    Returns:
        None
    """
    if not os.path.exists(DATA_FILE):
        logging.info("No existing data found. Starting new store.")
        return

    try:
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as file:
            for raw_line in file:
                parts: List[str] = raw_line.strip().split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    # Update or insert the key in memory
                    for i, (k, _) in enumerate(index):
                        if k == key:
                            index[i] = (key, value)
                            break
                    else:
                        index.append((key, value))
    except (OSError, UnicodeDecodeError) as e:
        logging.error(f"Failed to read {DATA_FILE}: {e}")
    except Exception as e:
        logging.exception(f"Unexpected error in load_data(): {e}")


class KeyValueStore:
    """
    A simple persistent key-value store.

    Responsibilities:
        - Manage an in-memory index
        - Append operations to a persistent log file
        - Provide safe GET and SET functionality
    """

    def __init__(self) -> None:
        """Initialize the store and rebuild index from persistent log."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Store or update a key-value pair in the log and memory.

        Args:
            key (str): The key to store.
            value (str): The value to associate with the key.

        Returns:
            None

        Raises:
            KVError: If file write operation fails.
        """
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as file:
                file.write(f"SET {key} {value}\n")
                file.flush()
                os.fsync(file.fileno())

            for i, (k, _) in enumerate(self.index):
                if k == key:
                    self.index[i] = (key, value)
                    break
            else:
                self.index.append((key, value))

            print("OK", flush=True)
            logging.info(f"SET successful for key '{key}'")

        except OSError as e:
            logging.error(f"I/O error during SET for key '{key}': {e}")
            raise KVError("Write failed") from e

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve a stored value for a given key.

        Args:
            key (str): The key to look up.

        Returns:
            Optional[str]: The value if found, otherwise None.
        """
        for k, v in self.index:
            if k == key:
                logging.info(f"GET success for key '{key}'")
                return v
        logging.info(f"GET missing key '{key}'")
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a raw command line input into command and arguments.

    Args:
        line (str): The user's input string.

    Returns:
        Tuple[str, List[str]]: The command (uppercased) and list of arguments.
    """
    parts: List[str] = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def main() -> None:
    """
    Main interactive loop for the persistent key-value store.

    Returns:
        None
    """
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8"
    )

    store: KeyValueStore = KeyValueStore()
    logging.info("KVStore initialized successfully.")

    while True:
        try:
            sys.stdout.write("> ")
            sys.stdout.flush()
            user_input: str = sys.stdin.readline()
            if not user_input:
                break

            cmd, args = _parse_command(user_input)

            if cmd == "SET":
                if len(args) != 2:
                    print("ERR: Usage SET <key> <value>", flush=True)
                    continue
                store.set(args[0], args[1])

            elif cmd == "GET":
                if len(args) != 1:
                    print("ERR: Usage GET <key>", flush=True)
                    continue
                result: Optional[str] = store.get(args[0])
                print(result if result is not None else "NULL", flush=True)

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
            logging.exception(f"Unhandled exception: {e}")
            print(f"ERR: Unexpected {e}", flush=True)
        finally:
            sys.stdout.flush()


if __name__ == "__main__":
    main()


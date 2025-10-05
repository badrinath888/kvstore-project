"""
Persistent Key-Value Store — Final Polished 100% Version
=========================================================
Implements a simple append-only persistent key-value database.

Supports:
  - SET <key> <value>
  - GET <key>
  - EXIT

Features:
  - UTF-8 safe I/O
  - Context-managed file operations
  - Logging and exception handling
  - In-memory index reconstruction
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
    Used for issues such as invalid commands or file I/O problems.
    """
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Load existing key-value pairs from the persistent log file.

    Args:
        index (List[Tuple[str, str]]): The in-memory list used to store keys and values.

    Notes:
        - Lines are parsed as: SET <key> <value>
        - The most recent value for each key replaces older entries.
        - Errors are logged and do not stop the application.
    """
    if not os.path.exists(DATA_FILE):
        logging.info("No previous data found. Starting new store.")
        return
    try:
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as file:
            for raw_line in file:
                parts = raw_line.strip().split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    # Update or insert in-memory record
                    for i, (k, _) in enumerate(index):
                        if k == key:
                            index[i] = (key, value)
                            break
                    else:
                        index.append((key, value))
    except (OSError, UnicodeDecodeError) as e:
        logging.error(f"Failed to load data from {DATA_FILE}: {e}")
    except Exception as e:
        logging.exception(f"Unexpected load_data() error: {e}")


class KeyValueStore:
    """
    The core key-value store class.

    Handles:
      - Storing and retrieving persistent data
      - Managing an in-memory index
      - Writing commands to the append-only log
    """

    def __init__(self) -> None:
        """Initialize the key-value store and load existing records."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Store or update a key-value pair in the database.

        Args:
            key (str): The key name.
            value (str): The value to store.

        Raises:
            KVError: If writing to the data file fails.
        """
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as file:
                file.write(f"SET {key} {value}\n")
                file.flush()
                os.fsync(file.fileno())

            # Update or append to in-memory index
            for i, (k, _) in enumerate(self.index):
                if k == key:
                    self.index[i] = (key, value)
                    break
            else:
                self.index.append((key, value))

            print("OK", flush=True)
            logging.info(f"SET successful for key '{key}'")

        except OSError as e:
            logging.error(f"I/O write error for '{key}': {e}")
            raise KVError("Write operation failed") from e

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve a stored value for a given key.

        Args:
            key (str): The key to look up.

        Returns:
            Optional[str]: The stored value, or None if not found.
        """
        for k, v in self.index:
            if k == key:
                logging.info(f"GET retrieved '{key}'")
                return v
        logging.info(f"GET missing key '{key}'")
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a command entered by the user.

    Args:
        line (str): The raw input string.

    Returns:
        Tuple[str, List[str]]: The command (uppercased) and argument list.
    """
    parts = line.strip().split()
    return (parts[0].upper(), parts[1:]) if parts else ("", [])


def main() -> None:
    """
    Main program loop for the interactive key-value store.

    Handles user input commands, dispatching to the appropriate methods,
    and ensures robust error handling with graceful exit.
    """
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8"
    )

    store = KeyValueStore()
    logging.info("KVStore initialized and running.")

    while True:
        try:
            sys.stdout.write("> ")
            sys.stdout.flush()
            command_line = sys.stdin.readline()
            if not command_line:
                break

            cmd, args = _parse_command(command_line)

            if cmd == "SET":
                if len(args) != 2:
                    print("ERR: Usage SET <key> <value>", flush=True)
                    continue
                store.set(args[0], args[1])

            elif cmd == "GET":
                if len(args) != 1:
                    print("ERR: Usage GET <key>", flush=True)
                    continue
                result = store.get(args[0])
                print(result if result is not None else "NULL", flush=True)

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
            logging.exception(f"Unhandled exception: {e}")
            print(f"ERR: Unexpected {e}", flush=True)
        finally:
            sys.stdout.flush()


if __name__ == "__main__":
    main()


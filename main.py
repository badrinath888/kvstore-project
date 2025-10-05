import os
import sys
import logging
from typing import List, Tuple, Optional


DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


class KVError(Exception):
    """
    Custom exception for key-value store errors.

    Raised when file I/O, command processing, or validation fails.
    """
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Load key-value pairs from the persistent append-only data file.

    Args:
        index (List[Tuple[str, str]]): The in-memory list holding key-value pairs.

    This function reconstructs the in-memory index by replaying the log file.
    It logs detailed error information if any exceptions occur during loading.
    """
    if not os.path.exists(DATA_FILE):
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
                    # Update or append key-value pair
                    for i, (k, _) in enumerate(index):
                        if k == key:
                            index[i] = (key, value)
                            break
                    else:
                        index.append((key, value))
    except (OSError, UnicodeDecodeError) as e:
        logging.error(f"Error loading {DATA_FILE}: {e}", exc_info=True)
        raise KVError(f"File read failed: {e}")
    except Exception as e:
        # Explicitly log and rethrow unexpected errors
        logging.critical(f"Unexpected error during load_data: {e}", exc_info=True)
        raise KVError(f"Unexpected load error: {e}") from e


class KeyValueStore:
    """
    A simple persistent key-value store.

    Supports SET, GET, and EXIT commands via a command-line interface (CLI).
    Data persists across sessions using an append-only log file.
    """

    def __init__(self) -> None:
        """Initialize the in-memory index and load previously persisted data."""
        self.index: List[Tuple[str, str]] = []
        try:
            load_data(self.index)
        except KVError as e:
            logging.error(f"Initialization warning: {e}")

    def set(self, key: str, value: str) -> None:
        """
        Store a key-value pair persistently and update the in-memory index.

        Args:
            key (str): The key to store.
            value (str): The value to associate with the key.

        Raises:
            KVError: If writing to the data file fails.
        """
        try:
            with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
                f.write(f"SET {key} {value}\n")
                f.flush()
                os.fsync(f.fileno())

            for i, (k, _) in enumerate(self.index):
                if k == key:
                    self.index[i] = (key, value)
                    break
            else:
                self.index.append((key, value))

            print("OK", flush=True)
            logging.info(f"SET successful: key={key}")
        except (OSError, ValueError) as e:
            logging.error(f"SET failed for key={key}: {e}", exc_info=True)
            raise KVError(f"SET failed: {e}")
        except Exception as e:
            logging.critical(f"Unexpected SET error for key={key}: {e}", exc_info=True)
            raise KVError(f"Unexpected SET error: {e}") from e

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value associated with a given key.

        Args:
            key (str): The key to search for.

        Returns:
            Optional[str]: The stored value, or None if not found.
        """
        for k, v in self.index:
            if k == key:
                logging.info(f"GET success: key={key}")
                return v
        logging.warning(f"GET: key '{key}' not found.")
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a user input line into a command and list of arguments.

    Args:
        line (str): Raw input string from the user.

    Returns:
        Tuple[str, List[str]]: The parsed command and its arguments.
    """
    parts = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def main() -> None:
    """
    Run the interactive command-line key-value store interface.

    Supports:
      - SET <key> <value>
      - GET <key>
      - EXIT
    """
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

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
                val = store.get(args[0])
                print(val if val is not None else "NULL", flush=True)

            elif cmd == "EXIT":
                print("BYE", flush=True)
                break

            elif cmd:
                print(f"ERR: Unknown command '{cmd}'", flush=True)

        except KVError as e:
            print(f"ERR: {e}", flush=True)
        except KeyboardInterrupt:
            print("\nBYE", flush=True)
            break
        except Exception as e:
            logging.critical(f"Unhandled error: {e}", exc_info=True)
            print(f"ERR: Unexpected error occurred: {e}", flush=True)
        finally:
            sys.stdout.flush()


if __name__ == "__main__":
    main()


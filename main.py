import os
import sys
import logging
from typing import List, Tuple, Optional


DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


class KVError(Exception):
    """Custom exception for key-value store errors."""
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Rebuilds in-memory index by replaying the persistent log file.

    Args:
        index (List[Tuple[str, str]]): List storing (key, value) tuples.

    This ensures durability across restarts by reading all 'SET' operations
    recorded in the append-only data file.
    """
    if not os.path.exists(DATA_FILE):
        logging.info("No existing data file found, starting fresh.")
        return
    try:
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                parts = line.split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    for i, (k, _) in enumerate(index):
                        if k == key:
                            index[i] = (key, value)
                            break
                    else:
                        index.append((key, value))
        logging.info("Data loaded successfully from file.")
    except (UnicodeDecodeError, OSError) as e:
        logging.error(f"Error loading data: {e}", exc_info=True)
    except Exception as e:
        logging.exception(f"Unexpected error during data load: {e}")


class KeyValueStore:
    """A simple persistent key-value store with durable append-only logging."""

    def __init__(self) -> None:
        """Initialize the store and rebuild from disk if available."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair in the log and update memory.

        Args:
            key (str): The key name.
            value (str): The associated value.
        """
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
            logging.info(f"SET executed successfully for key='{key}'")
        except OSError as e:
            raise KVError(f"File I/O error while writing key '{key}': {e}")
        except Exception as e:
            raise KVError(f"Unexpected error during SET operation: {e}")

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve a value for a given key if it exists.

        Args:
            key (str): The key to retrieve.

        Returns:
            Optional[str]: The value if found, else None.
        """
        try:
            for k, v in self.index:
                if k == key:
                    logging.info(f"GET success for key='{key}'")
                    return v
            logging.info(f"GET miss for key='{key}'")
            return None
        except Exception as e:
            logging.error(f"Error retrieving key '{key}': {e}", exc_info=True)
            return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse raw CLI input into a command and its arguments.

    Args:
        line (str): The user input string.

    Returns:
        Tuple[str, List[str]]: (command, list of arguments)
    """
    parts = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def main() -> None:
    """Run the interactive CLI for the key-value store."""
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    logging.info("KVStore session started.")
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
                logging.info("Session exited cleanly.")
                break

            elif cmd:
                print(f"ERR: Unknown command '{cmd}'", flush=True)
                logging.warning(f"Invalid command attempted: {cmd}")

        except KVError as e:
            print(f"ERR: {e}", flush=True)
            logging.error(f"KVError: {e}")
        except KeyboardInterrupt:
            print("\nBYE", flush=True)
            logging.info("Session interrupted by user.")
            break
        except Exception as e:
            print(f"ERR: Unexpected system error: {e}", flush=True)
            logging.exception(f"Unhandled exception: {e}")


if __name__ == "__main__":
    main()


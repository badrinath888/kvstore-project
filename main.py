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
    Load persisted data into memory from the append-only file.

    Args:
        index (List[Tuple[str, str]]): The in-memory list of key-value pairs.
    """
    if not os.path.exists(DATA_FILE):
        logging.info("No previous data found; initializing new store.")
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
        logging.info("Data successfully reloaded from file.")
    except Exception as e:
        logging.exception(f"Failed to load data: {e}")


class KeyValueStore:
    """A simple, persistent, append-only key-value store."""

    def __init__(self) -> None:
        """Initialize the in-memory store and rebuild from file."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Save a key-value pair persistently.

        Args:
            key (str): Key name.
            value (str): Value to store.
        """
        if not key.strip():
            raise KVError("Key cannot be empty.")
        if not isinstance(value, str):
            raise KVError("Value must be a string.")

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
            logging.info(f"SET successful for key='{key}' value='{value}'")
        except Exception as e:
            logging.exception(f"Error writing key='{key}': {e}")
            raise KVError(f"Unable to save data: {e}")

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value for a given key.

        Args:
            key (str): Key name.

        Returns:
            Optional[str]: The stored value or None if not found.
        """
        if not key.strip():
            logging.warning("Attempted GET with empty key.")
            return None

        for k, v in self.index:
            if k == key:
                logging.info(f"GET successful for key='{key}', value='{v}'")
                return v
        logging.info(f"GET miss for key='{key}'")
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse user input into command and arguments.

    Args:
        line (str): CLI input.

    Returns:
        Tuple[str, List[str]]: The command and argument list.
    """
    parts = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def main() -> None:
    """Run the command-line interface for the key-value store."""
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    logging.info("KVStore started successfully.")
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
                    logging.warning(f"Invalid SET syntax: {args}")
                    continue
                store.set(args[0], args[1])

            elif cmd == "GET":
                if len(args) != 1:
                    print("ERR: Usage GET <key>", flush=True)
                    logging.warning(f"Invalid GET syntax: {args}")
                    continue
                val = store.get(args[0])
                print(val if val is not None else "NULL", flush=True)

            elif cmd == "EXIT":
                print("BYE", flush=True)
                logging.info("User exited session cleanly.")
                break

            elif cmd:
                print(f"ERR: Unknown command '{cmd}'", flush=True)
                logging.warning(f"Unknown command received: '{cmd}'")

        except KVError as e:
            print(f"ERR: {e}", flush=True)
            logging.error(f"KVError: {e}")
        except KeyboardInterrupt:
            print("\nBYE", flush=True)
            logging.info("KeyboardInterrupt: User terminated session.")
            break
        except Exception as e:
            print(f"ERR: Unexpected system error: {e}", flush=True)
            logging.exception(f"Unhandled exception: {e}")
        finally:
            sys.stdout.flush()


if __name__ == "__main__":
    main()


import os
import sys
import logging
from typing import List, Tuple, Optional


DATA_FILE = "data.db"
LOG_FILE = "kvstore.log"


class KVError(Exception):
    """Custom exception for key-value store operations."""
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Rebuild the in-memory index by replaying the append-only data file.

    Args:
        index (List[Tuple[str, str]]): In-memory key-value index to update.
    """
    if not os.path.exists(DATA_FILE):
        return

    try:
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for raw_line in f:
                line = raw_line.strip()
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
    except Exception as e:
        logging.error(f"Error loading data: {e}", exc_info=True)


class KeyValueStore:
    """
    A persistent key-value store with append-only storage and in-memory indexing.
    """

    def __init__(self) -> None:
        """Initialize the key-value store and load existing data."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Store or update a key-value pair persistently.

        Args:
            key (str): The key to set.
            value (str): The value to store.
        """
        if not key.strip():
            raise KVError("Key cannot be empty.")
        if " " in key:
            raise KVError("Key cannot contain spaces.")

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

            logging.info(f"SET executed for key='{key}'")
            print("OK", flush=True)

        except Exception as e:
            logging.error(f"Error writing key='{key}': {e}", exc_info=True)
            raise KVError(f"Failed to write data: {e}")

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the stored value for a given key.

        Args:
            key (str): The key to look up.
        Returns:
            Optional[str]: The stored value or None if not found.
        """
        for k, v in self.index:
            if k == key:
                logging.info(f"GET successful for key='{key}'")
                return v
        logging.warning(f"GET requested for missing key='{key}'")
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse user input into a command and list of arguments.

    Args:
        line (str): The input line.
    Returns:
        Tuple[str, List[str]]: Command and arguments.
    """
    parts = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def main() -> None:
    """Run the interactive key-value store command interface."""
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8"
    )

    store = KeyValueStore()
    print("KeyValueStore ready. Use SET <key> <value>, GET <key>, or EXIT.", flush=True)

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
                logging.info("Program exited by user.")
                break

            elif cmd:
                print(f"ERR: Unknown command '{cmd}'", flush=True)
                logging.warning(f"Unknown command received: {cmd}")

        except KVError as e:
            print(f"ERR: {e}", flush=True)
        except KeyboardInterrupt:
            print("\nBYE", flush=True)
            break
        except Exception as e:
            logging.error(f"Unexpected error: {e}", exc_info=True)
            print(f"ERR: Unexpected {e}", flush=True)
        finally:
            sys.stdout.flush()


if __name__ == "__main__":
    main()


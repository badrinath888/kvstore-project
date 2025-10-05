import os
import sys
import logging
from typing import List, Tuple, Optional

DATA_FILE = "data.db"
LOG_FILE = "kvstore.log"


class KVError(Exception):
    """Custom exception for key-value store errors."""
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Load data from the persistent file into memory.

    Args:
        index (List[Tuple[str, str]]): In-memory key-value index.
    """
    if not os.path.exists(DATA_FILE):
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
    except (OSError, UnicodeDecodeError) as e:
        logging.error(f"Error reading data file: {e}")
    except Exception as e:
        raise KVError(f"Unexpected load error: {e}") from e


class KeyValueStore:
    """A simple persistent key-value store using an append-only file."""

    def __init__(self) -> None:
        """Initialize in-memory index and load existing data."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair and update the in-memory index.

        Args:
            key (str): The key to store.
            value (str): The value to associate with the key.
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
        except OSError as e:
            raise KVError(f"Disk write failed: {e}") from e

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve a value for the given key.

        Args:
            key (str): The key to retrieve.
        Returns:
            Optional[str]: The corresponding value or None if not found.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


def parse_and_execute(store: KeyValueStore, line: str) -> bool:
    """
    Parse a single command line and execute the corresponding action.

    Args:
        store (KeyValueStore): The key-value store instance.
        line (str): The raw input command.

    Returns:
        bool: False if EXIT command is given, True otherwise.
    """
    cmd, *args = line.strip().split(maxsplit=2)
    cmd = cmd.upper() if cmd else ""

    if cmd == "SET":
        if len(args) != 2:
            print("ERR: Usage SET <key> <value>", flush=True)
            return True
        store.set(args[0], args[1])

    elif cmd == "GET":
        if len(args) != 1:
            print("ERR: Usage GET <key>", flush=True)
            return True
        val = store.get(args[0])
        print(val if val is not None else "NULL", flush=True)

    elif cmd == "EXIT":
        print("BYE", flush=True)
        return False

    elif cmd:
        print(f"ERR: Unknown command '{cmd}'", flush=True)

    return True


def main() -> None:
    """Main interactive loop for the key-value store."""
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8"
    )

    store = KeyValueStore()
    print("> Welcome to the KV Store (type EXIT to quit)", flush=True)

    while True:
        try:
            sys.stdout.write("> ")
            sys.stdout.flush()
            line = sys.stdin.readline()
            if not line:
                break
            if not parse_and_execute(store, line):
                break
        except KVError as e:
            logging.error(f"KVError: {e}")
            print(f"ERR: {e}", flush=True)
        except KeyboardInterrupt:
            print("\nBYE", flush=True)
            break
        except (OSError, ValueError) as e:
            logging.error(f"System error: {e}")
            print(f"ERR: System error - {e}", flush=True)
        finally:
            sys.stdout.flush()


if __name__ == "__main__":
    main()


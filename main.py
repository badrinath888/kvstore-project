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
    Load key-value data from the persistent file into memory.

    Args:
        index (List[Tuple[str, str]]): In-memory list of key-value pairs.
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
    """A simple persistent key-value store using append-only storage."""

    def __init__(self) -> None:
        """Initialize the store and rebuild the in-memory index from disk."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Persist a key-value pair and update the in-memory index.

        Args:
            key (str): Key name.
            value (str): Value associated with the key.
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
        Retrieve a value for a given key.

        Args:
            key (str): The key to search for.

        Returns:
            Optional[str]: The stored value, or None if not found.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


def parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a raw command input line.

    Args:
        line (str): The user input line.

    Returns:
        Tuple[str, List[str]]: A tuple of command and its arguments.
    """
    parts: List[str] = line.strip().split(maxsplit=2)
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def execute_command(store: KeyValueStore, cmd: str, args: List[str]) -> bool:
    """
    Execute a parsed command on the key-value store.

    Args:
        store (KeyValueStore): Instance of the key-value store.
        cmd (str): The command to execute.
        args (List[str]): Arguments for the command.

    Returns:
        bool: False if the user chose to exit, True otherwise.
    """
    if cmd == "SET":
        if len(args) != 2:
            print("ERR: Usage SET <key> <value>", flush=True)
            return True
        store.set(args[0], args[1])

    elif cmd == "GET":
        if len(args) != 1:
            print("ERR: Usage GET <key>", flush=True)
            return True
        value: Optional[str] = store.get(args[0])
        print(value if value is not None else "NULL", flush=True)

    elif cmd == "EXIT":
        print("BYE", flush=True)
        return False

    elif cmd:
        print(f"ERR: Unknown command '{cmd}'", flush=True)

    return True


def main() -> None:
    """
    Entry point for the interactive key-value store.

    Handles user input, command parsing, and execution loop.
    """
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8"
    )

    store: KeyValueStore = KeyValueStore()
    print("> Welcome to the Key-Value Store! Type EXIT to quit.", flush=True)

    while True:
        try:
            sys.stdout.write("> ")
            sys.stdout.flush()
            line: str = sys.stdin.readline()
            if not line:
                break
            cmd, args = parse_command(line)
            if not execute_command(store, cmd, args):
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


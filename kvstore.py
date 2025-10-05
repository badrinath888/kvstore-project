import os
import sys
import logging
from typing import List, Tuple, Optional


DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


class KVError(Exception):
    """Custom exception class for all key-value store errors."""
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Rebuild the in-memory key-value index by replaying the append-only log file.

    Args:
        index (List[Tuple[str, str]]): The list storing key-value pairs in memory.

    This function ensures data persistence across restarts and recovers
    the last known state of the store.
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
        logging.error(f"Data load error: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"Unexpected error loading data: {e}", exc_info=True)


class KeyValueStore:
    """
    Persistent append-only key-value store with in-memory indexing.

    Features:
        - Persistent storage via append-only file
        - Crash recovery via log replay
        - In-memory index for fast retrieval
    """

    def __init__(self) -> None:
        """Initialize the in-memory index and load existing data."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Store or update a key-value pair persistently.

        Args:
            key (str): The key name.
            value (str): The value associated with the key.
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
            raise KVError(f"File write failed: {e}") from e
        except Exception as e:
            raise KVError(f"Unexpected error during SET: {e}") from e

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value for a given key.

        Args:
            key (str): The key to look up.

        Returns:
            Optional[str]: The stored value, or None if the key does not exist.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a command-line input into a command and its arguments.

    Args:
        line (str): The input line from the user.

    Returns:
        Tuple[str, List[str]]: A tuple containing the command and its arguments.
    """
    parts = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def main() -> None:
    """
    Launch the interactive command-line interface for the key-value store.

    Commands:
        SET <key> <value>  - Store or update a key-value pair
        GET <key>          - Retrieve a value
        EXIT               - Exit the program safely
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
            print(f"ERR: Unexpected {e}", flush=True)
        finally:
            sys.stdout.flush()


if __name__ == "__main__":
    main()


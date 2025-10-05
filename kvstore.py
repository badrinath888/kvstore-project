import os
import sys
import logging
from typing import List, Tuple, Optional


DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


class KVError(Exception):
    """Custom exception class for KeyValueStore-specific errors."""
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Replay append-only log from DATA_FILE to rebuild the in-memory index.

    Args:
        index (List[Tuple[str, str]]): The in-memory index list to populate.

    Raises:
        KVError: If there is an issue reading or decoding the data file.
    """
    if not os.path.exists(DATA_FILE):
        return

    try:
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for raw_line in f:
                line: str = raw_line.strip()
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
        raise KVError(f"Error reading {DATA_FILE}: {e}")
    except Exception as e:
        raise KVError(f"Unexpected error during load_data: {e}")


class KeyValueStore:
    """
    A simple persistent key-value store with append-only logging.

    Features:
        - Persistent writes to disk (append-only log)
        - In-memory index reconstruction on startup
        - Supports SET, GET, and EXIT commands
        - UTF-8 safe storage
    """

    def __init__(self) -> None:
        """Initialize the in-memory index and load persisted data."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Store or update a key-value pair persistently.

        Args:
            key (str): The key name.
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
        except (OSError, IOError) as e:
            raise KVError(f"File write failed: {e}")
        except Exception as e:
            raise KVError(f"Unexpected error in set(): {e}")

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value for a given key.

        Args:
            key (str): The key name to look up.

        Returns:
            Optional[str]: The stored value if found, otherwise None.
        """
        for k, v in self.index:
            if k == key:
                return v
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse a line of input into a command and arguments.

    Args:
        line (str): The raw input string.

    Returns:
        Tuple[str, List[str]]: Command name and list of arguments.
    """
    parts: List[str] = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def main() -> None:
    """
    Run the interactive key-value store CLI.

    Handles user input for SET, GET, and EXIT commands.
    Logs all actions and errors to kvstore.log.
    """
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    store = KeyValueStore()

    while True:
        try:
            sys.stdout.write("> ")
            sys.stdout.flush()
            line: str = sys.stdin.readline()
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
            logging.error(str(e))
        except KeyboardInterrupt:
            print("\nBYE", flush=True)
            break
        except Exception as e:
            err_msg = f"Unexpected error: {e}"
            print(f"ERR: {err_msg}", flush=True)
            logging.error(err_msg)
        finally:
            sys.stdout.flush()


if __name__ == "__main__":
    main()

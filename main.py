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
    Replay the append-only log to rebuild in-memory index.

    Args:
        index (List[Tuple[str, str]]): List to store key-value pairs.
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
        logging.error(f"Error loading data file: {e}", exc_info=True)
    except Exception as e:
        logging.exception(f"Unexpected error while loading data: {e}")


class KeyValueStore:
    """A simple persistent key-value store supporting SET, GET, and EXIT commands."""

    def __init__(self) -> None:
        """Initialize an empty index and load existing data."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Store a key-value pair persistently.

        Args:
            key (str): The key name.
            value (str): The value to associate.
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
            logging.info(f"SET key={key}")
        except OSError as e:
            raise KVError(f"I/O error while writing data: {e}")
        except Exception as e:
            raise KVError(f"Unexpected write failure: {e}")

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value for a given key.

        Args:
            key (str): The key to look up.

        Returns:
            Optional[str]: The value if found, otherwise None.
        """
        for k, v in self.index:
            if k == key:
                logging.info(f"GET key={key} found")
                return v
        logging.info(f"GET key={key} not found")
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse the user input into command and arguments.

    Args:
        line (str): Raw input from the user.

    Returns:
        Tuple[str, List[str]]: Command name and list of arguments.
    """
    parts = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def main() -> None:
    """Run the interactive key-value store CLI."""
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
            print(f"ERR: Unexpected error: {e}", flush=True)
            logging.exception(f"Unhandled error: {e}")


if __name__ == "__main__":
    main()


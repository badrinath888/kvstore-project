import os
import sys
import logging
from typing import List, Tuple, Optional


DATA_FILE: str = "data.db"
LOG_FILE: str = "kvstore.log"


class KVError(Exception):
    """
    Custom exception for key-value store errors.
    Raised for any controlled application-level failure (I/O, parsing, etc.).
    """
    pass


def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Load key-value pairs from persistent storage into memory.

    Args:
        index (List[Tuple[str, str]]): In-memory storage list of key-value pairs.

    The function replays the append-only log file to rebuild the store.
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
                    # Update if key exists, else append new pair
                    for i, (k, _) in enumerate(index):
                        if k == key:
                            index[i] = (key, value)
                            break
                    else:
                        index.append((key, value))
    except (OSError, UnicodeDecodeError) as e:
        logging.error(f"File read error: {e}", exc_info=True)
        raise KVError(f"Failed to load data: {e}")
    except Exception as e:
        # Catch unexpected issues, but still log for debugging
        logging.critical(f"Unexpected load error: {e}", exc_info=True)
        raise KVError(f"Unexpected load error: {e}") from e


class KeyValueStore:
    """Simple persistent key-value store supporting SET and GET commands."""

    def __init__(self) -> None:
        """Initialize memory index and load any existing data."""
        self.index: List[Tuple[str, str]] = []
        try:
            load_data(self.index)
        except KVError as e:
            logging.warning(f"Initialization issue: {e}")

    def set(self, key: str, value: str) -> None:
        """Persist a key-value pair and update the in-memory index."""
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
            logging.info(f"SET successful for key={key}")
        except (OSError, ValueError) as e:
            logging.error(f"SET failed for key={key}: {e}", exc_info=True)
            raise KVError(f"SET failed: {e}")
        except Exception as e:
            logging.critical(f"Unexpected error on SET: {e}", exc_info=True)
            raise KVError(f"Unexpected error on SET: {e}") from e

    def get(self, key: str) -> Optional[str]:
        """Retrieve value for a given key if it exists."""
        for k, v in self.index:
            if k == key:
                logging.debug(f"GET success: key={key}")
                return v
        logging.warning(f"GET: key not found: {key}")
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """
    Parse user input into command and argument list.
    Handles excessive whitespace or invalid input gracefully.
    """
    line = line.strip()
    if not line:
        return "", []
    parts = [p for p in line.split() if p]  # filter extra spaces
    cmd = parts[0].upper()
    args = parts[1:]

    # Prevent unexpected long input from breaking logic
    if len(args) > 5:
        logging.warning(f"Command has too many arguments: {parts}")
        args = args[:5]
    return cmd, args


def main() -> None:
    """
    Run an interactive loop to accept commands:
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

            if not cmd:
                continue

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

            else:
                print(f"ERR: Unknown command '{cmd}'", flush=True)
                logging.warning(f"Unknown command received: {cmd}")

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


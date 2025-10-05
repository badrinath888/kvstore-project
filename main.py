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
    Rebuild the in-memory index from the append-only data file.
    Handles UTF-8 safely and ignores malformed lines.
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
        logging.error(f"File read error: {e}")
    except Exception as e:
        raise KVError(f"Unexpected data load error: {e}") from e


class KeyValueStore:
    """
    Simple persistent key-value store.
    Uses append-only file persistence and in-memory indexing.
    """

    def __init__(self) -> None:
        """Initialize index and load existing data."""
        self.index: List[Tuple[str, str]] = []
        load_data(self.index)

    def set(self, key: str, value: str) -> None:
        """
        Store a key-value pair persistently.
        Raises:
            KVError: If writing to disk fails.
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
            raise KVError(f"File system write failed: {e}") from e
        except ValueError as e:
            raise KVError(f"Invalid data format: {e}") from e

    def get(self, key: str) -> Optional[str]:
        """Retrieve value for a key, if it exists."""
        for k, v in self.index:
            if k == key:
                return v
        return None


def _parse_command(line: str) -> Tuple[str, List[str]]:
    """Split a CLI input into command and arguments."""
    parts = line.strip().split()
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def main() -> None:
    """Run the interactive key-value store."""
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8"
    )

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
            logging.error(str(e))
            print(f"ERR: {e}", flush=True)
        except KeyboardInterrupt:
            print("\nBYE", flush=True)
            break
        except (OSError, ValueError) as e:
            print(f"ERR: System error - {e}", flush=True)
        finally:
            sys.stdout.flush()


if __name__ == "__main__":
    main()


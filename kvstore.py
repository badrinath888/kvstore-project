# file: kvstore.py
import sys
import os
from typing import List, Tuple, Optional

DATA_FILE = "data.db"


class KeyValueStore:
    """Append-only persistent key-value store.

    Design:
      • Log file: human-readable lines "SET <key> <value>\n" in data.db
      • Index: in-memory list[(key, value)] (no dict/map per assignment)
      • Read: linear scan for key (last-write-wins semantics)
      • Recovery: replay data.db at startup

    Constraints:
      • Values may contain spaces (parsed with split(maxsplit=2)).
      • Durability: flush + fsync per SET.
      • Invalid lines are ignored (robust recovery).
    """

    def __init__(self) -> None:
        """Initialize storage and rebuild the in-memory index.

        If data.db exists, all valid lines are replayed to reconstruct state.
        """
        self.index: List[Tuple[str, str]] = []
        self.load_data()

    def load_data(self) -> None:
        """Replay the append-only log into memory.

        Each "SET key value" line is applied in order.
        Older values are overwritten by newer ones.
        """
        if not os.path.exists(DATA_FILE):
            return
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                parts = line.strip().split(" ", 2)
                if len(parts) == 3 and parts[0] == "SET":
                    _, key, value = parts
                    self._set_in_memory(key, value)

    def _set_in_memory(self, key: str, value: str) -> None:
        """Insert or update (key, value) in memory."""
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str) -> None:
        """Persist and index a SET command.

        Steps:
          1. Append "SET key value" to data.db
          2. Flush and fsync for durability
          3. Update in-memory index
        """
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
            f.write(f"SET {safe_key} {safe_value}\n")
            f.flush()
            os.fsync(f.fileno())
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> Optional[str]:
        """Return the most recent value for key or None if missing."""
        for k, v in self.index:
            if k == key:
                return v
        return None


def _err() -> None:
    """Write a standardized error message to stdout."""
    sys.stdout.buffer.write(b"ERR\n")
    sys.stdout.flush()


def main() -> None:
    """Command-line interface for the KV store.

    Commands:
      SET <key> <value> → store/update key
      GET <key>         → retrieve value or NULL
      EXIT              → quit program
    """
    store = KeyValueStore()
    for line in sys.stdin:
        parts = line.strip().split(" ", 2)
        if not parts:
            continue
        cmd = parts[0].upper()

        if cmd == "EXIT":
            break
        elif cmd == "SET" and len(parts) == 3:
            _, key, value = parts
            store.set(key, value)
        elif cmd == "GET" and len(parts) == 2:
            _, key = parts
            value = store.get(key)
            if value is not None:
                sys.stdout.buffer.write((value + "\n").encode("utf-8", errors="replace"))
            else:
                sys.stdout.buffer.write(b"NULL\n")
            sys.stdout.flush()
        else:
            _err()


if __name__ == "__main__":
    main()

# file: kv_store.py
import sys
import os
import io

DATA_FILE = "data.db"

class KeyValueStore:
    def __init__(self):
        self.index = []
        self.load_data()

    def load_data(self):
        if not os.path.exists(DATA_FILE):
            return
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                parts = line.strip().split(" ", 2)
                if len(parts) == 3 and parts[0] == "SET":
                    _, key, value = parts
                    self._set_in_memory(key, value)

    def _set_in_memory(self, key: str, value: str):
        for i, (k, _) in enumerate(self.index):
            if k == key:
                self.index[i] = (key, value)
                return
        self.index.append((key, value))

    def set(self, key: str, value: str):
        safe_value = value.encode("utf-8", errors="replace").decode("utf-8")
        safe_key = key.encode("utf-8", errors="replace").decode("utf-8")
        with open(DATA_FILE, "a", encoding="utf-8", errors="replace") as f:
            f.write(f"SET {safe_key} {safe_value}\n")
            f.flush()
            os.fsync(f.fileno())
        self._set_in_memory(safe_key, safe_value)

    def get(self, key: str) -> str | None:
        for k, v in self.index:
            if k == key:
                return v
        return None

def main():
    # UTF-8 safe stdin/stdout for Gradebot
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

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
            print(value if value is not None else "NULL", flush=True)
        else:
            print("ERR", flush=True)

if __name__ == "__main__":
    main()

# KV Store Project 1 — Simple Append-Only Key-Value Store

**Course:** CSCE 5350  
**Author:** Badrinath  
**EUID:** 11820168  

---

## 📖 Overview

A tiny but **persistent** key–value store that demonstrates core database ideas:

- **Append-only log** in `data.db` (crash-safe, simple recovery)
- **Replay on startup** to rebuild an **in-memory index**
- **Last-write-wins** semantics
- Simple **CLI** with `SET`, `GET`, and `EXIT`

This is intentionally minimal to match Project 1 constraints (no built-in dict/map for the index).

---

## 🗂 What’s in the Code (file-by-file)

### `kvstore.py` — main program
- **Constants**
  - `DATA_FILE = "data.db"` — append-only log on disk
- **Exception**
  - `KVError` — for consistent, user-facing CLI errors (e.g., wrong arguments)
- **Store**
  - `class KeyValueStore`
    - `self.index: List[Tuple[str, str]]` — **list of (key, value)** pairs (no dict).  
      The *rightmost* matching key is the most recent value.
    - `load_data()` — **replays** `data.db` on startup to rebuild `index`.  
      Accepts lines of the form `SET <key> <value>`; skips malformed lines. Files are opened with `encoding="utf-8", errors="replace"` for robustness.
    - `set(key: str, value: str)` — appends `SET key value\n` to `data.db`, then `flush()` + `os.fsync()` for durability, and updates the in-memory index (algorithm below).
    - `get(key: str) -> Optional[str]` — linear scan of the index to return the latest value (or `None`).
- **CLI/REPL helpers**
  - `_parse_command(line: str) -> Tuple[str, List[str]]` — splits a command line into `(CMD, args)`. CMD uppercased; args kept as-is.
  - `_write_line(text: str)` — UTF-8 safe output to STDOUT.
  - `_err(msg: str)` — prints standardized errors: `ERR: <message>`.
- **`main()`** — REPL: reads from STDIN and handles `SET/GET/EXIT`.

### `load_data.py` — replay helper (optional)
- `load_data(index: List[Tuple[str, str]]) -> None`  
  Importable version of log replay for cleaner separation of concerns.

### `test_kv_store.py` — sanity tests
- Covers:
  - `SET/GET` happy path
  - Overwrite semantics (last-write-wins)
  - Nonexistent key returns `NULL`
  - Basic command parsing

---

## 💾 On-Disk Format (append-only)

Each write is one line:

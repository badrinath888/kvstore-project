# KV Store Project 1 — Simple Append-Only Key-Value Store
**Course:** CSCE 5350  
**Author:** Badrinath | **EUID:** 11820168  

---

## Overview
This project implements a **persistent key-value store** that supports the commands:
- `SET <key> <value>`  
- `GET <key>`  
- `EXIT`  

Data is persisted in an **append-only log** (`data.db`). On restart, the store replays the log to rebuild the in-memory index. The design enforces **last-write-wins semantics**, so the most recent value for a key is always returned.

---

## Features
- Append-only file storage (`data.db`)
- Linear in-memory index (no built-in dict/map)
- Crash recovery by log replay
- Immediate durability via flush + fsync
- Robust CLI with informative error handling

---

## Example Usage
```bash
$ python3 kvstore.py
SET name Badrinath_11820168
GET name
Badrinath_11820168
SET course CSCE5350
GET course
CSCE5350
EXIT

git add README.md
git commit -m "Docs: expanded README with examples & EUID (CSCE 5350, Badrinath 11820168)"
git push origin main

exit
cd ~/kvstore-project

echo -e "data.db\n__pycache__/" >> .gitignore
git rm --cached data.db || true
git rm -r --cached __pycache__ || true
git add .gitignore
git commit -m "Cleanup: ignore runtime files (Badrinath, EUID 11820168)"
git push origin main





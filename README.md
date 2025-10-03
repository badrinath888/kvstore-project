# KV Store Project 1 — Simple Append-Only Key-Value Store

**Course:** CSCE 5350  
**Author:** Badrinath  
**EUID:** 11820168  

---

## 📖 Overview
This project implements a **persistent key-value store** with a simple command-line interface (CLI).  
It supports the following commands:

- `SET <key> <value>` → Store or update a key-value pair.  
- `GET <key>` → Retrieve the value for a key. Returns `NULL` if not found.  
- `EXIT` → Quit the program.  

Data is stored in an **append-only log** (`data.db`).  
On restart, the log is replayed to rebuild the in-memory index.  
The store enforces **last-write-wins** semantics.

---

## ✨ Features
- Persistent storage in `data.db` (append-only writes).  
- In-memory index (implemented without built-in dict/map).  
- Crash recovery via log replay.  
- Immediate durability (using `flush + fsync`).  
- Robust CLI with **clear error messages**.  
- **Logging system** for debugging and maintainability.  
- Extensive unit tests, covering **normal cases and edge cases**.  

---

## ⚙️ Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/badrinath888/kvstore-project.git
   cd kvstore-project

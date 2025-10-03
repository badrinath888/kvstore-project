# KV Store Project 1 — Simple Append-Only Key-Value Store

**Course:** CSCE 5350  
**Author:** Badrinath  
**EUID:** 11820168  

---

## 📖 Overview
This project implements a **persistent key-value store** that supports the commands:

- `SET <key> <value>`
- `GET <key>`
- `EXIT`

Data is persisted in an **append-only log** (`data.db`).  
On restart, the store **replays the log** to rebuild the in-memory index.  
The design enforces **last-write-wins** semantics, so the most recent value for a key is always returned.

---

## ✨ Features
- Append-only file storage (`data.db`)
- Linear in-memory index (no built-in dict/map)
- Crash recovery by log replay
- Immediate durability via `flush + fsync`
- Robust CLI with **informative error handling**
- Extensive unit tests, including **edge cases**

---

## ⚙️ Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/badrinath888/kvstore-project.git
   cd kvstore-project





# KV Store Project 1 — Simple Append-Only Key-Value Store

**Course:** CSCE 5350  
**Author:** Badrinath  
**EUID:** 11820168  

---

## 📖 Overview
This project implements a **persistent key-value database** that uses an **append-only log file** (`data.db`) for durability and an in-memory index for quick lookups.  
The program is completely CLI-based and demonstrates how real databases ensure crash safety and recovery.

---

## ✨ Features
- `SET <key> <value>` – Store or update a key-value pair  
- `GET <key>` – Retrieve a value (`NULL` if not found)  
- `EXIT` – Quit gracefully  
- Data durability with `flush()` + `os.fsync()`  
- UTF-8-safe I/O  
- Custom `KVError` exceptions  
- Automatic replay of `data.db` on startup  
- Structured logging to `kvstore.log`

---

## ⚙️ Installation & Setup

1. **Clone the repository and enter the project:**
   ```bash
   git clone https://github.com/badrinath888/kvstore-project.git
   cd kvstore-project

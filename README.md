# KV Store Project 1 — Simple Append-Only Key-Value Store

**Course:** CSCE 5350  
**Author:** Badrinath  
**EUID:** 11820168  

---

## 📖 Overview
This project implements a **persistent key-value store** with a simple command-line interface (CLI).  
The store supports the following commands:

- `SET <key> <value>` — Store or overwrite a value for a given key  
- `GET <key>` — Retrieve the most recent value for a given key  
- `EXIT` — Exit the program safely  

Data is persisted in an **append-only log file** (`data.db`).  
On restart, the program **replays the log** to rebuild the in-memory index.  
The design enforces **last-write-wins** semantics, so the most recent value for a key is always returned.  

---

## ✨ Features
- Append-only file storage (`data.db`)  
- Crash recovery by log replay  
- Immediate durability using `flush + fsync`  
- In-memory index (no built-in dict/map)  
- Robust CLI with **clear error messages**  
- Automated unit tests covering **normal and edge cases**  

---

## ⚙️ Installation

### 1. Prerequisites
- Python **3.8 or higher**  
- Git installed  

### 2. Clone the Repository
```bash
git clone https://github.com/badrinath888/kvstore-project.git
cd kvstore-project

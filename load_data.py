import os
import logging
from typing import List, Tuple

DATA_FILE: str = "data.db"

def load_data(index: List[Tuple[str, str]]) -> None:
    """
    Replay append-only log to rebuild in-memory index.
    Improved error handling for robustness.
    """
    if not os.path.exists(DATA_FILE):
        return
    try:
        with open(DATA_FILE, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                line: str = raw.strip()
                if not line:
                    continue
                parts: List[str] = line.split(maxsplit=2)
                if len(parts) == 3 and parts[0].upper() == "SET":
                    _, key, value = parts
                    # Update or insert key-value in memory
                    for i, (k, _) in enumerate(index):
                        if k == key:
                            index[i] = (key, value)
                            break
                    else:
                        index.append((key, value))
    except UnicodeDecodeError as ude:
        logging.error(f"Unicode decode error in {DATA_FILE}: {ude}")
    except OSError as ose:
        logging.error(f"OS error loading {DATA_FILE}: {ose}")
    except Exception as e:
        logging.error(f"Unexpected error loading {DATA_FILE}: {e}")

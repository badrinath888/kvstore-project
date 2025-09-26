# file: test_kv_store.py
import subprocess
import os
import sys
import time

DB_FILE = "data.db"


def run_cmds(cmds):
    """Run kv_store.py with a list of commands, return output lines"""
    proc = subprocess.Popen(
        [sys.executable, "kv_store.py"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,  # ensures UTF-8 text mode
    )
    out, err = proc.communicate("\n".join(cmds) + "\n")
    return out.strip().splitlines(), err


def clean_db():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)


def test_set_get():
    clean_db()
    out, err = run_cmds([
        "SET a 1",
        "GET a",
        "EXIT",
    ])
    assert out == ["1"], f"Unexpected: {out}, err={err}"


def test_overwrite():
    clean_db()
    out, err = run_cmds([
        "SET a 1",
        "SET a 2",
        "GET a",
        "EXIT",
    ])
    assert out == ["2"], f"Unexpected: {out}"


def test_nonexistent():
    clean_db()
    out, err = run_cmds([
        "GET missing",
        "EXIT",
    ])
    assert out == ["NULL"], f"Unexpected: {out}"


def test_persistence():
    clean_db()
    run_cmds(["SET a 42", "EXIT"])
    out, err = run_cmds(["GET a", "EXIT"])
    assert out == ["42"], f"Unexpected after restart: {out}"


def test_invalid_utf8():
    clean_db()
    # Send invalid bytes through stdin (simulate Gradebot edge case)
    bad_bytes = b"SET bad \xff\nGET bad\nEXIT\n"
    proc = subprocess.Popen(
        [sys.executable, "kv_store.py"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = proc.communicate(bad_bytes)
    decoded_out = out.decode("utf-8", errors="replace").strip().splitlines()
    assert decoded_out[-2] in ("NULL", ""), f"Unexpected UTF-8 handling: {decoded_out}"


def run_all():
    test_set_get()
    print("✔ SET/GET passed")
    test_overwrite()
    print("✔ Overwrite passed")
    test_nonexistent()
    print("✔ Nonexistent key passed")
    test_persistence()
    print("✔ Persistence passed")
    test_invalid_utf8()
    print("✔ Invalid UTF-8 handled safely")


if __name__ == "__main__":
    run_all()
    print("\nAll tests passed ✅")

#!/usr/bin/env python3
# Tests for KV Store Project 1
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import subprocess
import tempfile
import pytest

KVSTORE = ["python3", "kvstore.py"]


def run_kvstore(commands):
    """Helper: run kvstore.py with commands, capture stdout."""
    process = subprocess.Popen(
        KVSTORE,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, _ = process.communicate("\n".join(commands) + "\n")
    return stdout.strip().splitlines()


def test_set_get(tmp_path):
    os.chdir(tmp_path)
    out = run_kvstore(["SET name Alice", "GET name", "EXIT"])
    assert out == ["Alice"]


def test_overwrite(tmp_path):
    os.chdir(tmp_path)
    out = run_kvstore(["SET name Alice", "SET name Bob", "GET name", "EXIT"])
    assert out == ["Bob"]


def test_nonexistent_get(tmp_path):
    os.chdir(tmp_path)
    out = run_kvstore(["GET missing", "EXIT"])
    assert out == ["NULL"]


def test_persistence(tmp_path):
    os.chdir(tmp_path)
    dbfile = tmp_path / "data.db"
    run_kvstore(["SET course CSCE5350", "EXIT"])
    assert dbfile.exists()
    out = run_kvstore(["GET course", "EXIT"])
    assert out == ["CSCE5350"]


def test_blank_and_exit(tmp_path):
    os.chdir(tmp_path)
    out = run_kvstore(["", "EXIT"])
    assert out == []  # blank line ignored


def test_malformed_set(tmp_path):
    os.chdir(tmp_path)
    out = run_kvstore(["SET onlykey", "EXIT"])
    assert out[0].startswith("ERR:")


def test_get_extra_args(tmp_path):
    os.chdir(tmp_path)
    out = run_kvstore(["GET key extra", "EXIT"])
    assert out[0].startswith("ERR:")





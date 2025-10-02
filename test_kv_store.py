#!/usr/bin/env python3
# test_kv_store.py — Unit tests for KeyValueStore CLI
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import shutil
import subprocess
import tempfile
import unittest


KV_CMD = ["python3", "kvstore.py"]


def run_cli(commands, cwd):
    """Run kvstore.py with a list of command lines; return stdout lines."""
    proc = subprocess.Popen(
        KV_CMD,
        cwd=cwd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, _ = proc.communicate("\n".join(commands) + "\n")
    out_lines = [ln for ln in stdout.splitlines() if ln.strip() != ""]
    return out_lines


class TestKVStoreCLI(unittest.TestCase):
    def setUp(self):
        # Use a temp directory so each test has its own clean data.db
        self.tmpdir = tempfile.mkdtemp(prefix="kv_cli_")
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

    def test_set_get(self):
        out = run_cli(["SET name Badrinath_11820168", "GET name", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["Badrinath_11820168"])

    def test_overwrite_last_write_wins(self):
        out = run_cli(["SET k v1", "SET k v2", "SET k v3", "GET k", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["v3"])

    def test_nonexistent(self):
        out = run_cli(["GET missing", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["NULL"])

    def test_persistence_across_restart(self):
        # First run writes, second run reads
        run_cli(["SET course CSCE5350", "EXIT"], cwd=self.tmpdir)
        out = run_cli(["GET course", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["CSCE5350"])

    def test_blank_line_ignored(self):
        out = run_cli(["", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, [])  # no output expected

    def test_malformed_set(self):
        out = run_cli(["SET onlykey", "EXIT"], cwd=self.tmpdir)
        self.assertTrue(out[0].startswith("ERR:") or out[0].startswith("ERR"))

    def test_get_extra_arg(self):
        out = run_cli(["GET key extra", "EXIT"], cwd=self.tmpdir)
        self.assertTrue(out[0].startswith("ERR:") or out[0].startswith("ERR"))

    def test_unicode_values(self):
        out = run_cli(["SET greet こんにちは", "GET greet", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["こんにちは"])


if __name__ == "__main__":
    unittest.main(verbosity=2)






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

def run_cli(lines, cwd):
    """Run kvstore.py with command lines; return stdout lines (non-empty)."""
    proc = subprocess.Popen(
        KV_CMD,
        cwd=cwd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, _ = proc.communicate("\n".join(lines) + "\n")
    return [ln for ln in stdout.splitlines() if ln.strip() != ""]

class TestKVStoreCLI(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="kv_cli_")
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

    # ---- Basic Required Tests ----
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
        run_cli(["SET course CSCE5350", "EXIT"], cwd=self.tmpdir)
        out = run_cli(["GET course", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["CSCE5350"])

    # ---- Edge Case Tests ----
    def test_blank_line_ignored(self):
        out = run_cli(["", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, [])

    def test_malformed_set_missing_value(self):
        out = run_cli(["SET onlykey", "EXIT"], cwd=self.tmpdir)
        self.assertTrue(out[0].startswith("ERR"))

    def test_get_extra_arg(self):
        out = run_cli(["GET key extra", "EXIT"], cwd=self.tmpdir)
        self.assertTrue(out[0].startswith("ERR"))

    def test_set_extra_arg(self):
        out = run_cli(["SET key v1 extra", "EXIT"], cwd=self.tmpdir)
        self.assertTrue(out[0].startswith("ERR"))

    def test_empty_key(self):
        out = run_cli(["SET  value_only", "EXIT"], cwd=self.tmpdir)
        self.assertTrue(any("ERR" in line for line in out))

    def test_empty_value(self):
        out = run_cli(["SET key ", "GET key", "EXIT"], cwd=self.tmpdir)
        self.assertTrue(any("ERR" in line for line in out))

    def test_unicode_key_value(self):
        out = run_cli(["SET café naïve", "GET café", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["naïve"])

    def test_long_key_and_value(self):
        long_key = "k" * 1000
        long_value = "v" * 2000
        out = run_cli([f"SET {long_key} {long_value}", f"GET {long_key}", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, [long_value])

    def test_case_insensitive_commands(self):
        out = run_cli(["set key value", "gEt key", "ExIt"], cwd=self.tmpdir)
        self.assertEqual(out, ["value"])

    def test_unknown_command(self):
        out = run_cli(["FOO bar", "EXIT"], cwd=self.tmpdir)
        self.assertTrue(out[0].startswith("ERR"))

    def test_multiple_spaces_between_args(self):
        out = run_cli(["SET   spaced    value", "GET spaced", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["value"])

    def test_exit_without_commands(self):
        out = run_cli(["EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, [])

if __name__ == "__main__":
    unittest.main(verbosity=2)




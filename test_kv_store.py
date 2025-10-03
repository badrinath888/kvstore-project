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
    """
    Run kvstore.py with a sequence of command lines.

    Args:
        lines (List[str]): Commands to send to CLI.
        cwd (str): Temporary working directory.

    Returns:
        List[str]: Output lines (excluding blanks).
    """
    proc = subprocess.Popen(
        KV_CMD,
        cwd=cwd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, stderr = proc.communicate("\n".join(lines) + "\n")
    if stderr.strip():
        # Capture unexpected errors for debugging
        print("STDERR:", stderr)
    return [ln for ln in stdout.splitlines() if ln.strip() != ""]


class TestKVStoreCLI(unittest.TestCase):
    def setUp(self):
        """Create a fresh temporary directory for each test run."""
        self.tmpdir = tempfile.mkdtemp(prefix="kv_cli_")
        self.addCleanup(lambda: shutil.rmtree(self.tmpdir, ignore_errors=True))

    # --- Basic functionality ---
    def test_set_get(self):
        out = run_cli(["SET name Badrinath_11820168", "GET name", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["Badrinath_11820168"])

    def test_overwrite_last_write_wins(self):
        out = run_cli(["SET k v1", "SET k v2", "SET k v3", "GET k", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["v3"])

    def test_nonexistent_key_returns_null(self):
        out = run_cli(["GET missing", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["NULL"])

    def test_persistence_across_restart(self):
        run_cli(["SET course CSCE5350", "EXIT"], cwd=self.tmpdir)
        out = run_cli(["GET course", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["CSCE5350"])

    # --- CLI error handling ---
    def test_blank_line_ignored(self):
        out = run_cli(["", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, [])  # blank line produces no output

    def test_malformed_set_command(self):
        out = run_cli(["SET onlykey", "EXIT"], cwd=self.tmpdir)
        self.assertTrue(out[0].startswith("ERR"))

    def test_get_with_extra_argument(self):
        out = run_cli(["GET key extra", "EXIT"], cwd=self.tmpdir)
        self.assertTrue(out[0].startswith("ERR"))

    def test_unknown_command(self):
        out = run_cli(["FOO key value", "EXIT"], cwd=self.tmpdir)
        self.assertTrue(out[0].startswith("ERR"))

    # --- Edge cases ---
    def test_empty_key(self):
        out = run_cli(["SET '' value", "GET ''", "EXIT"], cwd=self.tmpdir)
        # Should store under literal empty string key
        self.assertEqual(out, ["value"])

    def test_empty_value(self):
        out = run_cli(["SET key ''", "GET key", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, ["''"])

    def test_whitespace_in_value(self):
        out = run_cli(["SET key 'hello world'", "GET key", "EXIT"], cwd=self.tmpdir)
        self.assertIn("hello world", out[0])

    def test_long_key_and_value(self):
        long_key = "k" * 1000
        long_val = "v" * 5000
        run_cli([f"SET {long_key} {long_val}", "EXIT"], cwd=self.tmpdir)
        out = run_cli([f"GET {long_key}", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, [long_val])

    def test_special_characters(self):
        special_key = "!@#$%^&*()"
        special_val = "🚀✨🔥"
        run_cli([f"SET {special_key} {special_val}", "EXIT"], cwd=self.tmpdir)
        out = run_cli([f"GET {special_key}", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, [special_val])

    def test_multiple_keys(self):
        cmds = [
            "SET a 1",
            "SET b 2",
            "SET c 3",
            "GET a",
            "GET b",
            "GET c",
            "EXIT",
        ]
        out = run_cli(cmds, cwd=self.tmpdir)
        self.assertEqual(out, ["1", "2", "3"])

    def test_repeated_sets_and_gets(self):
        cmds = ["SET repeat x", "SET repeat y", "SET repeat z", "GET repeat", "EXIT"]
        out = run_cli(cmds, cwd=self.tmpdir)
        self.assertEqual(out, ["z"])

    def test_case_sensitivity(self):
        run_cli(["SET Key Value", "EXIT"], cwd=self.tmpdir)
        out1 = run_cli(["GET Key", "EXIT"], cwd=self.tmpdir)
        out2 = run_cli(["GET key", "EXIT"], cwd=self.tmpdir)
        self.assertEqual(out1, ["Value"])
        self.assertEqual(out2, ["NULL"])  # Different casing not same key

    def test_exit_command_quits(self):
        out = run_cli(["EXIT"], cwd=self.tmpdir)
        self.assertEqual(out, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)

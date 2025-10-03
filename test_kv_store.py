#!/usr/bin/env python3
# Unit tests for KV Store Project 1
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import io
import os
import sys
import unittest
from unittest.mock import patch

import kvstore


class TestKeyValueStore(unittest.TestCase):
    """Tests for the core KeyValueStore functionality."""

    def setUp(self):
        # Remove test data file before each test
        if os.path.exists(kvstore.DATA_FILE):
            os.remove(kvstore.DATA_FILE)
        self.store = kvstore.KeyValueStore()

    def tearDown(self):
        if os.path.exists(kvstore.DATA_FILE):
            os.remove(kvstore.DATA_FILE)

    def test_set_and_get(self):
        self.store.set("name", "Badrinath")
        self.assertEqual(self.store.get("name"), "Badrinath")

    def test_overwrite_key(self):
        self.store.set("course", "CSCE5300")
        self.store.set("course", "CSCE5350")
        self.assertEqual(self.store.get("course"), "CSCE5350")

    def test_nonexistent_key(self):
        self.assertIsNone(self.store.get("missing"))

    def test_persistence(self):
        self.store.set("foo", "bar")
        # Reload from file
        s2 = kvstore.KeyValueStore()
        self.assertEqual(s2.get("foo"), "bar")

    def test_empty_key_rejected(self):
        with self.assertRaises(kvstore.KVError):
            self.store.set("", "value")

    def test_unicode_handling(self):
        key = "ключ"  # Russian word for "key"
        val = "значение"  # "value"
        self.store.set(key, val)
        self.assertEqual(self.store.get(key), val)

    def test_long_key_value(self):
        key = "k" * 1000
        val = "v" * 5000
        self.store.set(key, val)
        self.assertEqual(self.store.get(key), val)


class TestCLI(unittest.TestCase):
    """Tests for the command-line interface and error messages."""

    def run_cli(self, input_data: str) -> str:
        buf_in = io.StringIO(input_data)
        buf_out = io.StringIO()
        with patch("sys.stdin", buf_in), patch("sys.stdout", buf_out):
            kvstore.main()
        return buf_out.getvalue()

    def test_cli_set_and_get(self):
        out = self.run_cli("SET course CSCE5350\nGET course\nEXIT\n")
        self.assertIn("CSCE5350", out)

    def test_cli_nonexistent_key(self):
        out = self.run_cli("GET nope\nEXIT\n")
        self.assertIn("NULL", out)

    def test_cli_exit(self):
        out = self.run_cli("EXIT\n")
        self.assertEqual(out.strip(), "")

    def test_cli_set_missing_args(self):
        out = self.run_cli("SET onlykey\nEXIT\n")
        self.assertIn("ERR: SET requires exactly 2 arguments", out)

    def test_cli_set_too_many_args(self):
        out = self.run_cli("SET k v extra\nEXIT\n")
        self.assertIn("ERR: SET received too many arguments", out)

    def test_cli_get_wrong_args(self):
        out = self.run_cli("GET k extra\nEXIT\n")
        self.assertIn("ERR: GET requires exactly 1 argument", out)

    def test_cli_unknown_command(self):
        out = self.run_cli("DELETE k\nEXIT\n")
        self.assertIn("ERR: Unknown command", out)

    def test_cli_empty_line(self):
        out = self.run_cli("\nEXIT\n")
        self.assertEqual(out.strip(), "")

    def test_cli_unicode(self):
        out = self.run_cli("SET ключ значение\nGET ключ\nEXIT\n")
        self.assertIn("значение", out)

    def test_cli_long_value(self):
        long_val = "x" * 2000
        out = self.run_cli(f"SET k {long_val}\nGET k\nEXIT\n")
        self.assertIn(long_val, out)


if __name__ == "__main__":
    unittest.main()



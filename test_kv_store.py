#!/usr/bin/env python3
"""
Unit tests for KV Store Project 1
Course: CSCE 5350
Author: Badrinath | EUID: 11820168
"""

import io
import os
import sys
import unittest
from contextlib import redirect_stdout, redirect_stderr

import kvstore


class TestKeyValueStore(unittest.TestCase):

    def setUp(self):
        """Remove data.db before each test for isolation."""
        if os.path.exists(kvstore.DATA_FILE):
            os.remove(kvstore.DATA_FILE)
        self.store = kvstore.KeyValueStore()

    def test_set_and_get(self):
        self.store.set("foo", "bar")
        self.assertEqual(self.store.get("foo"), "bar")

    def test_overwrite_key(self):
        self.store.set("foo", "bar")
        self.store.set("foo", "baz")
        self.assertEqual(self.store.get("foo"), "baz")

    def test_nonexistent_key(self):
        self.assertIsNone(self.store.get("nope"))

    def test_persistence_after_restart(self):
        self.store.set("course", "CSCE5350")
        new_store = kvstore.KeyValueStore()
        self.assertEqual(new_store.get("course"), "CSCE5350")

    # --- Edge Case Tests ---

    def test_set_with_invalid_args(self):
        fake_in = io.StringIO("SET onlyonearg\nEXIT\n")
        fake_out = io.StringIO()
        with redirect_stdout(fake_out):
            sys.stdin = fake_in
            kvstore.main()
        sys.stdin = sys.__stdin__
        output = fake_out.getvalue()
        self.assertIn("ERR:", output)

    def test_get_with_invalid_args(self):
        fake_in = io.StringIO("GET key extra\nEXIT\n")
        fake_out = io.StringIO()
        with redirect_stdout(fake_out):
            sys.stdin = fake_in
            kvstore.main()
        sys.stdin = sys.__stdin__
        output = fake_out.getvalue()
        self.assertIn("ERR:", output)

    def test_unknown_command(self):
        fake_in = io.StringIO("HELLO key value\nEXIT\n")
        fake_out = io.StringIO()
        with redirect_stdout(fake_out):
            sys.stdin = fake_in
            kvstore.main()
        sys.stdin = sys.__stdin__
        output = fake_out.getvalue()
        self.assertIn("ERR:", output)

    def test_blank_lines(self):
        fake_in = io.StringIO("\n\nSET k v\nGET k\nEXIT\n")
        fake_out = io.StringIO()
        with redirect_stdout(fake_out):
            sys.stdin = fake_in
            kvstore.main()
        sys.stdin = sys.__stdin__
        output = fake_out.getvalue()
        self.assertIn("v", output)

    def test_long_key_and_value(self):
        key = "x" * 1000
        value = "y" * 5000
        self.store.set(key, value)
        self.assertEqual(self.store.get(key), value)

    def test_multiple_keys(self):
        for i in range(10):
            self.store.set(f"k{i}", f"v{i}")
        for i in range(10):
            self.assertEqual(self.store.get(f"k{i}"), f"v{i}")


if __name__ == "__main__":
    unittest.main()







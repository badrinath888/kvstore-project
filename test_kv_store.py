#!/usr/bin/env python3
# Tests for KV Store Project 1
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import io
import sys
import unittest
from kvstore import KeyValueStore, _parse_command, KVError, DATA_FILE


class TestKeyValueStore(unittest.TestCase):

    def setUp(self) -> None:
        """Remove existing data file before each test for isolation."""
        if os.path.exists(DATA_FILE):
            os.remove(DATA_FILE)
        self.store = KeyValueStore()

    def tearDown(self) -> None:
        if os.path.exists(DATA_FILE):
            os.remove(DATA_FILE)

    def test_set_and_get_basic(self) -> None:
        self.store.set("name", "Badrinath")
        self.assertEqual(self.store.get("name"), "Badrinath")

    def test_overwrite_key(self) -> None:
        self.store.set("x", "1")
        self.store.set("x", "2")
        self.assertEqual(self.store.get("x"), "2")

    def test_nonexistent_key(self) -> None:
        self.assertIsNone(self.store.get("ghost"))

    def test_persistence_after_restart(self) -> None:
        self.store.set("course", "CSCE5350")
        new_store = KeyValueStore()
        self.assertEqual(new_store.get("course"), "CSCE5350")

    # ---- Edge cases ----

    def test_empty_key(self) -> None:
        self.store.set("", "emptykey")
        self.assertEqual(self.store.get(""), "emptykey")

    def test_empty_value(self) -> None:
        self.store.set("key", "")
        self.assertEqual(self.store.get("key"), "")

    def test_unicode_support(self) -> None:
        self.store.set("emoji", "")
        self.assertEqual(self.store.get("emoji"), "")

    def test_long_key_and_value(self) -> None:
        long_key = "k" * 1000
        long_value = "v" * 5000
        self.store.set(long_key, long_value)
        self.assertEqual(self.store.get(long_key), long_value)

    def test_multiple_keys(self) -> None:
        self.store.set("a", "1")
        self.store.set("b", "2")
        self.assertEqual(self.store.get("a"), "1")
        self.assertEqual(self.store.get("b"), "2")

    def test_log_replay_skips_bad_lines(self) -> None:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            f.write("BADLINE without set\n")
            f.write("SET good test\n")
        new_store = KeyValueStore()
        self.assertEqual(new_store.get("good"), "test")


class TestParseCommand(unittest.TestCase):

    def test_parse_set(self) -> None:
        cmd, args = _parse_command("SET name value")
        self.assertEqual(cmd, "SET")
        self.assertEqual(args, ["name", "value"])

    def test_parse_get(self) -> None:
        cmd, args = _parse_command("GET name")
        self.assertEqual(cmd, "GET")
        self.assertEqual(args, ["name"])

    def test_parse_empty(self) -> None:
        cmd, args = _parse_command("   ")
        self.assertEqual(cmd, "")
        self.assertEqual(args, [])


class TestErrorHandling(unittest.TestCase):

    def test_invalid_command(self) -> None:
        store = KeyValueStore()
        captured = io.StringIO()
        sys.stdout = captured
        try:
            with self.assertRaises(KVError):
                raise KVError("unknown command")
        finally:
            sys.stdout = sys.__stdout__

    def test_invalid_args_set(self) -> None:
        cmd, args = _parse_command("SET onlykey")
        self.assertEqual(cmd, "SET")
        self.assertEqual(len(args), 1)


if __name__ == "__main__":
    unittest.main()


#!/usr/bin/env python3
# Tests for KV Store Project 1
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import unittest
from kvstore import KeyValueStore, KVError, DATA_FILE


class TestKeyValueStore(unittest.TestCase):
    def setUp(self):
        if os.path.exists(DATA_FILE):
            os.remove(DATA_FILE)
        self.store = KeyValueStore()

    def test_set_and_get(self):
        self.store.set("foo", "bar")
        self.assertEqual(self.store.get("foo"), "bar")

    def test_overwrite_key(self):
        self.store.set("foo", "bar")
        self.store.set("foo", "baz")
        self.assertEqual(self.store.get("foo"), "baz")

    def test_nonexistent_key(self):
        self.assertIsNone(self.store.get("not_there"))

    def test_empty_key(self):
        with self.assertRaises(KVError):
            self.store.set("", "value")
        with self.assertRaises(KVError):
            self.store.get("")

    def test_empty_value(self):
        with self.assertRaises(KVError):
            self.store.set("key", "")

    def test_long_key_and_value(self):
        long_key = "k" * 1000
        long_value = "v" * 5000
        self.store.set(long_key, long_value)
        self.assertEqual(self.store.get(long_key), long_value)

    def test_persistence(self):
        self.store.set("name", "Badrinath")
        new_store = KeyValueStore()
        self.assertEqual(new_store.get("name"), "Badrinath")


if __name__ == "__main__":
    unittest.main()


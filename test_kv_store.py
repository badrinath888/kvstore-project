#!/usr/bin/env python3
# test_kv_store.py — Unit tests for KeyValueStore
# Course: CSCE 5350
# Author: Badrinath | EUID: 11820168

import os
import unittest
from kvstore import KeyValueStore, DATA_FILE, KVError


class TestKeyValueStore(unittest.TestCase):
    def setUp(self) -> None:
        # Clean slate before each test
        if os.path.exists(DATA_FILE):
            os.remove(DATA_FILE)
        self.store = KeyValueStore()

    def tearDown(self) -> None:
        # Cleanup after each test
        if os.path.exists(DATA_FILE):
            os.remove(DATA_FILE)

    def test_set_and_get(self):
        """Basic SET and GET functionality."""
        self.store.set("name", "Badrinath_11820168")
        self.assertEqual(self.store.get("name"), "Badrinath_11820168")

    def test_overwrite_key(self):
        """Last write wins semantics: overwrite value for same key."""
        self.store.set("course", "CSCE5300")
        self.store.set("course", "CSCE5350")
        self.assertEqual(self.store.get("course"), "CSCE5350")

    def test_nonexistent_key(self):
        """GET on a key that does not exist should return None."""
        self.assertIsNone(self.store.get("does_not_exist"))

    def test_persistence_after_restart(self):
        """Values must survive restart (log replay)."""
        self.store.set("key1", "value1")
        # Simulate restart
        new_store = KeyValueStore()
        self.assertEqual(new_store.get("key1"), "value1")

    def test_multiple_keys(self):
        """Support multiple independent keys."""
        self.store.set("a", "1")
        self.store.set("b", "2")
        self.store.set("c", "3")
        self.assertEqual(self.store.get("a"), "1")
        self.assertEqual(self.store.get("b"), "2")
        self.assertEqual(self.store.get("c"), "3")

    def test_empty_value(self):
        """SET should handle empty string values gracefully."""
        self.store.set("blank", "")
        self.assertEqual(self.store.get("blank"), "")

    def test_unicode_values(self):
        """SET/GET must handle Unicode values correctly."""
        self.store.set("greeting", "こんにちは")  # Japanese for Hello
        self.assertEqual(self.store.get("greeting"), "こんにちは")


if __name__ == "__main__":
    unittest.main()




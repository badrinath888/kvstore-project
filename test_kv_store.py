import os
import unittest
from kvstore import KeyValueStore, DATA_FILE

class TestKeyValueStore(unittest.TestCase):

    def setUp(self):
        # Clean slate before each test
        if os.path.exists(DATA_FILE):
            os.remove(DATA_FILE)
        self.store = KeyValueStore()

    def test_set_and_get(self):
        self.store.set("name", "Alice")
        self.assertEqual(self.store.get("name"), "Alice")

    def test_overwrite_key(self):
        self.store.set("name", "Alice")
        self.store.set("name", "Bob")
        self.assertEqual(self.store.get("name"), "Bob")

    def test_nonexistent_key(self):
        self.assertIsNone(self.store.get("ghost"))

    def test_empty_value(self):
        self.store.set("empty", "")
        self.assertEqual(self.store.get("empty"), "")

    def test_special_characters_in_key(self):
        self.store.set("weird!@#", "value")
        self.assertEqual(self.store.get("weird!@#"), "value")

    def test_persistence_after_restart(self):
        self.store.set("course", "CSCE5350")
        # Re-initialize (simulate restart)
        new_store = KeyValueStore()
        self.assertEqual(new_store.get("course"), "CSCE5350")

if __name__ == "__main__":
    unittest.main()



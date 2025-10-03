"""
Unit tests for KVStore (CSCE 5350 Project 1).
Covers normal cases and additional edge cases.
"""

import os
import pytest
from kvstore import KVStore


@pytest.fixture
def store(tmp_path):
    """
    Creates a temporary KVStore for testing (uses temp data.db).
    """
    test_file = tmp_path / "data.db"
    return KVStore(filename=str(test_file))


def test_set_and_get(store):
    store.set("name", "Badrinath")
    assert store.get("name") == "Badrinath"


def test_overwrite_key(store):
    store.set("course", "CSCE")
    store.set("course", "CSCE5350")
    assert store.get("course") == "CSCE5350"


def test_nonexistent_key(store):
    assert store.get("doesnotexist") == "NULL"


def test_empty_key(store):
    store.set("", "empty_key")
    assert store.get("") == "empty_key"


def test_empty_value(store):
    store.set("key_only", "")
    assert store.get("key_only") == ""


def test_special_characters(store):
    store.set("sp&cial#k^y", "we!rd_val*ue")
    assert store.get("sp&cial#k^y") == "we!rd_val*ue"


def test_long_key(store):
    long_key = "k" * 1000
    store.set(long_key, "long_value")
    assert store.get(long_key) == "long_value"


def test_long_value(store):
    long_val = "v" * 5000
    store.set("long_val", long_val)
    assert store.get("long_val") == long_val


def test_replay_log(tmp_path):
    """
    Ensure persistence works after restart by replaying log.
    """
    dbfile = tmp_path / "data.db"
    s1 = KVStore(filename=str(dbfile))
    s1.set("persist", "yes")

    s2 = KVStore(filename=str(dbfile))
    assert s2.get("persist") == "yes"





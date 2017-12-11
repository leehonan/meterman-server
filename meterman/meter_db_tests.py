import random
import string
import time

from meterman import app_base as base
import pytest as pt

from meterman import meter_db as db

TEST_DB_FILE = base.TEMP_PATH + "meter_data_test.db"


@pt.fixture(scope="session")
def db_mgr():
    db_mgr = db.DBManager(TEST_DB_FILE)

    yield db_mgr  # provide the fixture value
    print("teardown db_mgr")
    db_mgr.close()


def test_insert_random_read(db_mgr):
    source_uuid = "99.99.99.99.1"
    timestamp_raw = int(time.time())
    timestamp = timestamp_raw
    timestamp_raw_nonce = "".join(random.choice(string.ascii_uppercase) for i in range(2))
    read_type = "test"
    read_value = 1
    rec_status = "test"
    db_mgr.write_meter_read(source_uuid, timestamp_raw, timestamp_raw_nonce, timestamp, read_type, read_value, rec_status)
    row = db_mgr.get_last_node_meter_read()
    assert row['source_uuid'] == source_uuid
    assert row['timestamp_raw'] == timestamp_raw
    assert row['timestamp_raw_nonce'] == timestamp_raw_nonce
    assert row['timestamp'] == timestamp
    assert row['read_type'] == read_type
    assert row['read_value'] == read_value
    assert row['rec_status'] == rec_status


def test_insert_random_read_then_delete(db_mgr):
    source_uuid = "99.99.99.99"
    timestamp_raw = int(time.time())
    timestamp = timestamp_raw
    timestamp_raw_nonce = "".join(random.choice(string.ascii_uppercase) for i in range(2))
    read_type = "test"
    read_value = 1
    rec_status = "test"
    db_mgr.write_meter_read(source_uuid, timestamp_raw, timestamp_raw_nonce, timestamp, read_type, read_value, rec_status)
    db_mgr.delete_node_meter_read(source_uuid, timestamp_raw, timestamp_raw_nonce)
    row = db_mgr.get_last_node_meter_read()
    assert row['timestamp_raw'] != timestamp_raw or row['timestamp_raw_nonce'] != timestamp_raw_nonce


def test_100_inserts():
    for i in range (1,100):
        test_insert_random_read(db_mgr)
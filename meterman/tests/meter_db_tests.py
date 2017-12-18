import random
import string
import time
import os

from meterman import app_base as base
import pytest as pt

from meterman import meter_db as db

TEST_DB_FILE = base.temp_path + "/meter_data_test.db"


@pt.fixture(scope="session")
def db_mgr():
    fixt_db_mgr = db.DBManager(TEST_DB_FILE)
    yield fixt_db_mgr  # provide the fixture value
    print("teardown db_mgr")
    fixt_db_mgr.conn_close()
    os.remove(TEST_DB_FILE)


def test_insert_random_read(db_mgr):
    node_uuid = "99.99.99.99.1"
    when_start_raw = int(time.time())
    when_start_raw_nonce = base.get_nonce()
    when_start = when_start_raw
    duration = 15
    entry_type = "MUPS"
    entry_value = 5
    meter_value = 15
    rec_status = "NORM"
    db_mgr.write_meter_entry(node_uuid, when_start_raw, when_start_raw_nonce, when_start, entry_type, entry_value, duration, meter_value, rec_status)
    row = db_mgr.get_last_mup(node_uuid, time_from=None, time_to=None)
    assert row['node_uuid'] == node_uuid
    assert row['when_start_raw'] == when_start_raw
    assert row['when_start_raw_nonce'] == when_start_raw_nonce
    assert row['when_start'] == when_start
    assert row['duration'] == duration
    assert row['entry_type'] == entry_type
    assert row['entry_value'] == entry_value
    assert row['meter_value'] == meter_value
    assert row['rec_status'] == rec_status


def test_insert_random_read_then_delete(db_mgr):
    node_uuid = "99.99.99.99.1"
    when_start_raw = int(time.time())
    when_start_raw_nonce = base.get_nonce()
    when_start = when_start_raw
    duration = 15
    entry_type = "MUPS"
    entry_value = 5
    meter_value = 15
    rec_status = "NORM"
    db_mgr.write_meter_entry(node_uuid, when_start_raw, when_start_raw_nonce, when_start, entry_type, entry_value, duration, meter_value, rec_status)
    db_mgr.delete_meter_entry(node_uuid, when_start_raw, when_start_raw_nonce)
    row = db_mgr.get_last_mup(node_uuid, time_from=None, time_to=None)
    assert row['when_start_raw'] != when_start_raw or row['when_start_raw_nonce'] != when_start_raw_nonce


def test_10_inserts(db_mgr):
    for i in range(10):
        test_insert_random_read(db_mgr)
        time.sleep(1)

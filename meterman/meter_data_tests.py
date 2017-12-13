import random
import string
import time
import os

from meterman import app_base as base, meter_db as db, meter_data_manager as mdm
import pytest as pt


TEST_DB_FILE = base.temp_path + "/meter_data_test.db"


@pt.fixture(scope="session")
def data_mgr():
    fixt_data_mgr = mdm.MeterDataManager(TEST_DB_FILE)
    yield fixt_data_mgr  # provide the fixture value
    print("teardown data_mgr")
    fixt_data_mgr.close_db()
    os.remove(TEST_DB_FILE)


def insert_cumulative_entries(data_mgr, node_uuid, start_time, entry_value, interval_duration, start_meter_value, num_entries):
    entry_time = start_time
    meter_value = start_meter_value + entry_value

    for i in range(num_entries):
        data_mgr.db_mgr.write_meter_entry(node_uuid, when_start_raw=entry_time,
                                 when_start_raw_nonce=base.get_nonce(), when_start=entry_time,
                                 entry_type='MUPS', entry_value=entry_value,
                                 duration=interval_duration, meter_value=meter_value, rec_status='NORM')
        entry_time += interval_duration
        meter_value += entry_value


def test_consumption_with_simple_entries(data_mgr):
    node_uuid = "99.99.99.99.1"
    start_time = base.MIN_TIME

    # generate 20 cumulative entries of 5Wh each
    insert_cumulative_entries(data_mgr, node_uuid, start_time=start_time, entry_value=5, interval_duration=60, start_meter_value=1000, num_entries=20)
    assert data_mgr.get_meter_consumption(node_uuid) == 95 # as starts at 1005
    data_mgr.db_mgr.delete_all_meter_entries(node_uuid)


def test_consumption_with_single_rebase_upfront(data_mgr):
    node_uuid = "99.99.99.99.1"
    start_time = base.MIN_TIME

    # write baseline
    data_mgr.db_mgr.write_meter_entry(node_uuid, when_start_raw=start_time,
                                      when_start_raw_nonce=base.get_nonce(), when_start=start_time,
                                      entry_type='MREBS', entry_value=0,
                                      duration=0, meter_value=1000, rec_status='NORM')

    # generate 20 cumulative entries of 5Wh each => 1100Wh
    insert_cumulative_entries(data_mgr, node_uuid, start_time=start_time, entry_value=5, interval_duration=60, start_meter_value=1000, num_entries=20)

    # consumption should be 1100 - 1000 == 100Wh
    assert data_mgr.get_meter_consumption(node_uuid) == 100
    data_mgr.db_mgr.delete_all_meter_entries(node_uuid)


def test_consumption_with_single_rebase_midway(data_mgr):
    node_uuid = "99.99.99.99.1"
    start_time = base.MIN_TIME

    # generate 20 cumulative entries of 5Wh each => 1100Wh, 95 observed
    insert_cumulative_entries(data_mgr, node_uuid, start_time=start_time, entry_value=5, interval_duration=60, start_meter_value=1000, num_entries=20)

    start_time += 1260

    data_mgr.db_mgr.write_meter_entry(node_uuid, when_start_raw=start_time,
                                      when_start_raw_nonce=base.get_nonce(), when_start=start_time,
                                      entry_type='MREBS', entry_value=0,
                                      duration=0, meter_value=1200, rec_status='NORM')

    # generate 20 cumulative entries of 5Wh each => 1300Wh
    insert_cumulative_entries(data_mgr, node_uuid, start_time=start_time, entry_value=5, interval_duration=60, start_meter_value=1200, num_entries=20)

    # consumption should be 1300 - 1200 + 95 == 195
    assert data_mgr.get_meter_consumption(node_uuid) == 195
    data_mgr.db_mgr.delete_all_meter_entries(node_uuid)


def test_consumption_with_single_rebase_at_end(data_mgr):
    node_uuid = "99.99.99.99.1"
    start_time = base.MIN_TIME

    # generate 20 cumulative entries of 5Wh each => 1100Wh
    insert_cumulative_entries(data_mgr, node_uuid, start_time=start_time, entry_value=5, interval_duration=60, start_meter_value=1000, num_entries=20)

    start_time += 1260

    data_mgr.db_mgr.write_meter_entry(node_uuid, when_start_raw=start_time,
                                      when_start_raw_nonce=base.get_nonce(), when_start=start_time,
                                      entry_type='MREBS', entry_value=0,
                                      duration=0, meter_value=1200, rec_status='NORM')

    # consumption should be 1200 - 1005 == 195
    assert data_mgr.get_meter_consumption(node_uuid) == 195
    data_mgr.db_mgr.delete_all_meter_entries(node_uuid)


def test_consumption_with_multiple_rebases_upfront_rebase(data_mgr):
    node_uuid = "99.99.99.99.1"
    start_time = base.MIN_TIME

    # write baseline
    data_mgr.db_mgr.write_meter_entry(node_uuid, when_start_raw=start_time,
                                      when_start_raw_nonce=base.get_nonce(), when_start=start_time,
                                      entry_type='MREBS', entry_value=0,
                                      duration=0, meter_value=1000, rec_status='NORM')

    # generate 5 cumulative entries of 5Wh each => 1025Wh
    insert_cumulative_entries(data_mgr, node_uuid, start_time=start_time, entry_value=5, interval_duration=60, start_meter_value=1000, num_entries=5)

    # rebase to 1100
    start_time += 360
    data_mgr.db_mgr.write_meter_entry(node_uuid, when_start_raw=start_time,
                                      when_start_raw_nonce=base.get_nonce(), when_start=start_time,
                                      entry_type='MREBS', entry_value=0,
                                      duration=0, meter_value=1100, rec_status='NORM')

    # generate 5 cumulative entries of 5Wh each => 1125Wh
    insert_cumulative_entries(data_mgr, node_uuid, start_time=start_time, entry_value=5, interval_duration=60, start_meter_value=1100, num_entries=5)

    # rebase to 1200
    start_time += 360
    data_mgr.db_mgr.write_meter_entry(node_uuid, when_start_raw=start_time,
                                      when_start_raw_nonce=base.get_nonce(), when_start=start_time,
                                      entry_type='MREBS', entry_value=0,
                                      duration=0, meter_value=1200, rec_status='NORM')

    # generate 10 cumulative entries of 5Wh each => 1250Wh
    insert_cumulative_entries(data_mgr, node_uuid, start_time=start_time, entry_value=5, interval_duration=60, start_meter_value=1200, num_entries=10)

    # consumption should be 1250 - 1000 == 250Wh
    assert data_mgr.get_meter_consumption(node_uuid) == 250
    data_mgr.db_mgr.delete_all_meter_entries(node_uuid)


def test_consumption_with_multiple_rebases_none_upfront(data_mgr):
    node_uuid = "99.99.99.99.1"
    start_time = base.MIN_TIME

    # generate 5 cumulative entries of 5Wh each => 1025Wh, 20Wh observed
    insert_cumulative_entries(data_mgr, node_uuid, start_time=start_time, entry_value=5, interval_duration=60, start_meter_value=1000, num_entries=5)

    # rebase to 1100
    start_time += 360
    data_mgr.db_mgr.write_meter_entry(node_uuid, when_start_raw=start_time,
                                      when_start_raw_nonce=base.get_nonce(), when_start=start_time,
                                      entry_type='MREBS', entry_value=0,
                                      duration=0, meter_value=1100, rec_status='NORM')

    # generate 5 cumulative entries of 5Wh each => 1125Wh
    insert_cumulative_entries(data_mgr, node_uuid, start_time=start_time, entry_value=5, interval_duration=60, start_meter_value=1100, num_entries=5)

    # rebase to 1200
    start_time += 360
    data_mgr.db_mgr.write_meter_entry(node_uuid, when_start_raw=start_time,
                                      when_start_raw_nonce=base.get_nonce(), when_start=start_time,
                                      entry_type='MREBS', entry_value=0,
                                      duration=0, meter_value=1200, rec_status='NORM')

    # generate 10 cumulative entries of 5Wh each => 1250Wh
    insert_cumulative_entries(data_mgr, node_uuid, start_time=start_time, entry_value=5, interval_duration=60, start_meter_value=1200, num_entries=10)

    # consumption should be 1250 - 1100 + 20 == 170Wh; starting at 1005 as there is no baseline read
    assert data_mgr.get_meter_consumption(node_uuid) == 170
    data_mgr.db_mgr.delete_all_meter_entries(node_uuid)

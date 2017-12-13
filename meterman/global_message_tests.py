import random
import string
import time
import serial
import os

from meterman import app_base as base
import pytest as pt

from meterman import meter_db as db
from meterman import meter_device_manager as mdm

TEST_DB_FILE = base.temp_path + "/meter_data_test.db"


@pt.fixture(scope="session")
def db_mgr():
    fixt_db_mgr = db.DBManager(TEST_DB_FILE)
    yield fixt_db_mgr  # provide the fixture value
    print("teardown db_mgr")
    fixt_db_mgr.conn_close()
    os.remove(TEST_DB_FILE)


@pt.fixture(scope="session")
def dev_mgr():
    print("\n\nAssumes socat running on /dev/ttys001 and /dev/ttys002")
    print("Start with socat -b 9600 -d -d pty,raw,echo=1 pty,raw,echo=1\n\n")
    gateway_config = {'network_id': '9.9.9.99', 'gateway_id': '1', 'label': 'Test Gateway', 'serial_port': '/dev/ttys001', 'serial_baud': '9600'}
    fixt_dev_mgr = mdm.MeterDeviceManager(None, gateway_config)
    yield fixt_dev_mgr  # provide the fixture value
    print("teardown dev_mgr")

@pt.fixture(scope="session")
def gw_serial_conn():
    fixt_gw_serial_conn = serial.Serial('/dev/ttys002', 9600, timeout=1, write_timeout=1)
    yield fixt_gw_serial_conn  # provide the fixture value
    print("teardown gw_serial_conn")
    fixt_gw_serial_conn.close()


def test_bad_messages(dev_mgr, gw_serial_conn):
    tx_msg = 'G>S:CRAP' + '\r\n'
    gw_serial_conn.write(tx_msg.encode('utf-8'))
    time.sleep(0.5)
    dev_mgr.gateways['9.9.9.99.1']['gw_obj'].rx_serial_msg()

    tx_msg = 'G>S:MUP_;2,MUP_,DEBUG:' + '\r\n'
    gw_serial_conn.write(tx_msg.encode('utf-8'))
    time.sleep(0.5)
    dev_mgr.gateways['9.9.9.99.1']['gw_obj'].rx_serial_msg()

    tx_msg = 'G>S:MUP_;2,MUP_,1496842913428,18829393;15,1;15,5;15,2;16,3;' + '\r\n'
    gw_serial_conn.write(tx_msg.encode('utf-8'))
    time.sleep(0.5)
    dev_mgr.gateways['9.9.9.99.1']['gw_obj'].rx_serial_msg()

    tx_msg = 'G>S:MUP_;2,MUP_,1496842913428,18829393;15,1;15,5;15,2;16,3' + '\r\n'
    gw_serial_conn.write(tx_msg.encode('utf-8'))
    time.sleep(0.5)
    dev_mgr.gateways['9.9.9.99.1']['gw_obj'].rx_serial_msg()

    assert True

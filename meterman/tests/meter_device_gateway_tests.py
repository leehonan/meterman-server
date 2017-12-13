from meterman import gateway_messages as gmsg
import pytest as pt

from meterman import meter_device_manager as devmgr

BASE_TIME = 1483228800        # Jan 1, 2017 (GMT)

@pt.fixture
def fixt_dev_mgr():
    dev_mgr = devmgr.MeterDeviceManager({'label': 'test_gateway', 'serial_port': '/dev/ttys003', 'serial_baud': 115200})
    return dev_mgr


@pt.fixture
def fixt_gateway(fixt_dev_mgr):
    gateway = fixt_dev_mgr.gateways['/dev/ttys003']
    return gateway


def test_meter_update_msg(fixt_gateway):
    meter_entries = []
    entry_time = BASE_TIME
    entry_value = 100000

    for i in range(7):
        meter_entries.append(gmsg.message_definitions['MTRUPDATE']['obj_detail_defn'](entry_time, entry_value))
        entry_time = entry_time + 15
        entry_value = entry_value + 10

    res = gmsg.meter_update_msg(2, meter_entries)

    msg_obj = gmsg.get_message_obj(res)

    # pass object to appropriate processor function using dictionary mapping
    handler = fixt_gateway.message_proc_functions[msg_obj['message_type']]
    getattr(fixt_gateway, handler)(msg_obj)


    # assert res == 'MTRUPDATE;{0},MUP;{1},{2};{3},{4};{5},{6};{7},{8};{9},{10};{11},{12};{13},{14}'.format(
    #                     2, BASE_TIME, 100000, 15, 10, 15, 10, 15, 10, 15, 10, 15, 10, 15, 10)
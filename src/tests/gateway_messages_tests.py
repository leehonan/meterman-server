from meterman import gateway_messages as gmsg

BASE_TIME = 1483228800        # Jan 1, 2017 (GMT)

def test_make_set_gateway_time_msg():
    res = gmsg.set_gateway_time_msg(BASE_TIME)
    assert res == 'SETTIME;{0}'.format(BASE_TIME)

def test_make_gateway_snapshot_msg():
    res = gmsg.gateway_snapshot_msg(1, BASE_TIME, 500, BASE_TIME, 'DEBUG', 'CHANGE_ME_PLEASE', '0.0.1.1', -3, 'Wh')
    assert res == 'GWSNAP;{0},{1},{2},{3},{4},{5},{6},{7},{8}'.format(
                        1, BASE_TIME, 500, BASE_TIME, 'DEBUG', 'CHANGE_ME_PLEASE', '0.0.1.1', -3, 'Wh')

def test_make_node_snapshot_msg():
    res = gmsg.node_snapshot_msg(2, 6000, 10000, 9000, 500, BASE_TIME, 1, 15, BASE_TIME, 155600, 0, 100, -56)
    assert res == 'NODESNAP;{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12}'.format(
                        2, 6000, 10000, 9000, 500, BASE_TIME, 1, 15, BASE_TIME, 155600, 0, 100, -56)

def test_make_meter_update_msg():
    meter_entries = []
    entry_time = BASE_TIME
    entry_value = 100000

    for i in range(0, 7):
        meter_entries.append(gmsg.message_definitions['MTRUPDATE']['obj_detail_defn'](entry_time, entry_value))
        entry_time = entry_time + 15
        entry_value = entry_value + 10

    res = gmsg.meter_update_msg(2, meter_entries)

    assert res == 'MTRUPDATE;{0},MUP;{1},{2};{3},{4};{5},{6};{7},{8};{9},{10};{11},{12};{13},{14}'.format(
                        2, BASE_TIME, 100000, 15, 10, 15, 10, 15, 10, 15, 10, 15, 10, 15, 10)


def test_make_meter_rebase_msg():
    res = gmsg.meter_rebase_msg(2, BASE_TIME, 100000)
    assert res == 'MTRREBASE;{0},MBASE;{1},{2}'.format(2, BASE_TIME, 100000)


def test_make_set_node_meter_value_msg():
    res = gmsg.set_node_meter_value_msg(2, 10)
    assert res == 'SETMTRVAL;{0},{1}'.format(2, 10)


def test_make_set_node_meter_interval_msg():
    res = gmsg.set_node_meter_interval_msg(2, 30)
    assert res == 'SETMTRINT;{0},{1}'.format(2, 30)


def test_make_set_node_puck_led_msg():
    res = gmsg.set_node_puck_led_msg(2, 1, 100)
    assert res == 'SETPLED;{0},{1},{2}'.format(2, 1, 100)


def test_meter_update_to_transfer_obj():
    res = gmsg.get_message_obj('MTRUPDATE;2,MUP;1483228800,100000;10,1;10,5;10,3')
    print(res)
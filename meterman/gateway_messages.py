'''

================================================================================================================================================================
gateway_messages.py
=====================

Static class for generating gateway messages from transfer objects, and vice-versa.  Separated from gateway implementation
to better support simulation and testing.


================================================================================================================================================================

'''

# ==============================================================================================================================================================
#  IMPORTS
# ==============================================================================================================================================================

import arrow
from enum import Enum
from collections import namedtuple
from random import randint

# ==============================================================================================================================================================
#  CONSTANTS
# ==============================================================================================================================================================
# Serial message and gateway attribute consts
SMSG_RX_PREFIX = "G>S:"
SMSG_TX_PREFIX = "S>G:"
SMSG_FS = ','
SMSG_RS = ';'
A_HEADER = 'HEADER'
A_DETAIL = 'DETAIL'
A_HEADER_SKIP = 'HEADER_SKIP'
A_DETAIL_SKIP = 'DETAIL_SKIP'
A_UNKNOWN = 'UNKNOWN'

# Message Directions
class MessageDirection(Enum):
    GW_TO_SVR = 0
    SVR_TO_GW = 1

# --------------------------------------------------------------------------------------------------------------------------------------------------------------
# Message Definitions:
# Attribute names are unique across header and detail.  Only one detail record type.
# --------------------------------------------------------------------------------------------------------------------------------------------------------------

message_definitions = {}

SMSG_GETTIME_DEFN = {
    #       format:     GTIME
    #       e.g.:       GTIME
    'smsg_type': 'GTIME',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type'],
    'smsg_attrib_type': [A_HEADER],
}

SMSG_SETTIME_DEFN = {
    # Sends request to gateway to set time to server's local time as Unix UTC Epoch.
    #       format:     STIME;<new_epoch_time_utc>
    #       e.g.:       STIME;1502795790
    'smsg_type': 'STIME',
    'smsg_direction': MessageDirection.SVR_TO_GW,
    'smsg_attrib_layout': ['smsg_type', 'new_epoch_time_utc'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER]
}

SMSG_SETTIME_ACK_DEFN = {
    #       format:     STIME_ACK
    #       e.g.:       STIME_ACK
    'smsg_type': 'STIME_ACK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type'],
    'smsg_attrib_type': [A_HEADER_SKIP]
}

SMSG_SETTIME_NACK_DEFN = {
    #       format:     STIME_NACK
    #       e.g.:       STIME_NACK
    'smsg_type': 'STIME_NACK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type'],
    'smsg_attrib_type': [A_HEADER_SKIP]
}

SMSG_GETGWSNAP_DEFN = {
    #  Requests a dump of the gateway's state.
    #       format:     GGWSNAP
    #       e.g.:       GGWSNAP
    'smsg_type': 'GGWSNAP',
    'smsg_direction': MessageDirection.SVR_TO_GW,
    'smsg_attrib_layout': ['smsg_type'],
    'smsg_attrib_type': [A_HEADER_SKIP]
}

SMSG_GWSNAP_DEFN = {
    #       format:     GWSNAP;<gateway_id>,<when_booted>,<free_ram>,<time>,<log_level>,<encrypt_key>,<network_id>,<tx_power>
    #       e.g.:       GWSNAP;1,1496842913428,577,1496842913428,DEBUG,PLEASE_CHANGE_ME,0.0.1.1,13

    'smsg_type': 'GWSNAP',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'gateway_id', 'when_booted', 'free_ram', 'gateway_time', 'log_level', 'encrypt_key', 'network_id',
                            'tx_power'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER, A_HEADER, A_HEADER, A_HEADER, A_HEADER, A_HEADER, A_HEADER, A_HEADER]
}

SMSG_SETGITR_DEFN = {
    # Sends request to gateway to set node's gateway instruction polling rate to more aggressive value for a temporary period.
    #       format:     SGITR;<node_id>,<tmp_poll_rate>,<tmp_poll_period>
    #       e.g.:       SGITR;2,30,300
    'smsg_type': 'SGITR',
    'smsg_direction': MessageDirection.SVR_TO_GW,
    'smsg_attrib_layout': ['smsg_type', 'node_id', 'tmp_poll_rate', 'tmp_poll_period'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER, A_HEADER, A_HEADER]
}

SMSG_SETGITR_ACK_DEFN = {
    #       format:     SGITR_ACK;<node_id>
    #       e.g.:       SGITR_ACK;2
    'smsg_type': 'SGITR_ACK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER]
}

SMSG_SETGITR_NACK_DEFN = {
    #       format:     SGITR_NACK;<node_id>
    #       e.g.:       SGITR_NACK;2
    'smsg_type': 'SGITR_NACK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER]
}

SMSG_GETNODESNAP_DEFN = {
    #  Requests a dump of a node's state from the Gateway.
    #       format:     GNOSNAP;<node_id>       # if no node or node=254 will return all
    #       e.g.:       GNOSNAP;2
    'smsg_type': 'GNOSNAP',
    'smsg_direction': MessageDirection.SVR_TO_GW,
    'smsg_attrib_layout': ['smsg_type', 'node_id'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER]
}

SMSG_NODESNAP_DEFN = {
    #       format:     NOSNAP;[1..n of [<node_id>,<batt_voltage>,<up_time>,<sleep_time>,<free_ram>,<when_last_seen>,<last_clock_drift>,
    #                       <meter_interval>,<meter_impulses_per_kwh>,<last_meter_entry_finish>,<last_meter_value>,<puck_led_rate>,<puck_led_time>,<last_rssi_at_gateway>]]
    #       e.g.:       NOSNAP;2,4500,15000,20000,600,1496842913428,500,5,1496842913428,3050,1,100,1000,-70
    'smsg_type': 'NOSNAP',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id', 'batt_voltage', 'up_time', 'sleep_time', 'free_ram', 'when_last_seen',
                           'last_clock_drift', 'meter_interval', 'meter_impulses_per_kwh', 'last_meter_entry_finish',
                           'last_meter_value', 'last_rms_current', 'puck_led_rate', 'puck_led_time',
                           'last_rssi_at_gateway'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_DETAIL, A_DETAIL, A_DETAIL, A_DETAIL, A_DETAIL, A_DETAIL,
                         A_DETAIL, A_DETAIL, A_DETAIL, A_DETAIL,
                         A_DETAIL, A_DETAIL, A_DETAIL, A_DETAIL,
                         A_DETAIL]
}

SMSG_GETNODESNAP_NACK_DEFN = {
    #       format:     GNOSNAP_NACK;<node_id>
    #       e.g.:       GNOSNAP_NACK;2
    'smsg_type': 'GNOSNAP_NACK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER]
}

SMSG_MTRUPDATE_WITH_IRMS_DEFN = {
    #       format:     MUPC;<node_id>,MUPC,<last_entry_finish_time>,<last_entry_meter_value>;
    #                   1..n of [<interval_duration>, <interval_value>, <spot_rms_current>]
    #       e.g.:       MUPC;2,MUPC,1496842913428,18829393;15,1,10.2;15,5,10.7;

    'smsg_type': 'MUPC',
    'rmsg_type': 'MUPC',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id', 'rmsg_type', 'last_entry_finish_time', 'last_entry_meter_value',
                           'entry_interval_length', 'entry_value', 'spot_rms_current'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER, A_HEADER_SKIP, A_HEADER, A_HEADER, A_DETAIL, A_DETAIL, A_DETAIL]
}

SMSG_MTRUPDATE_NO_IRMS_DEFN = {
    #       format:     MUP_;<node_id>,MUP_,1 of [<last_entry_finish_time>, <last_entry_meter_value>];
    #                   1..n of [<interval_duration>, <interval_value>];
    #       e.g.:       MUP_;2,MUP_,1496842913428,18829393;15,1;15,5;15,2;16,3;

    'smsg_type': 'MUP_',
    'rmsg_type': 'MUP_',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id', 'rmsg_type', 'last_entry_finish_time', 'last_entry_meter_value', 'entry_interval_length', 'entry_value'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER, A_HEADER_SKIP, A_HEADER, A_HEADER, A_DETAIL, A_DETAIL]
}

SMSG_MTRREBASE_DEFN = {
    #       format:     MREB;<node_id>,MREB,<entry_timestamp>,<meter_value>
    #       e.g.:       MREB;2,MREB,1496842913428,18829393
    'smsg_type': 'MREB',
    'rmsg_type': 'MREB',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id', 'rmsg_type', 'entry_timestamp', 'meter_value'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER, A_HEADER_SKIP, A_HEADER, A_HEADER]
}

SMSG_SETMTRVAL_DEFN = {
    #  Requests a reset of a node's meter value to the watt-hour value specified.
    #       format:     SMVAL;<node_id>,<new_meter_value>
    #       e.g.:       SMVAL;2,10
    'smsg_type': 'SMVAL',
    'smsg_direction': MessageDirection.SVR_TO_GW,
    'smsg_attrib_layout': ['smsg_type', 'node_id', 'new_meter_value'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER, A_HEADER]
}

SMSG_SETMTRVAL_ACK_DEFN = {
    #       format:     SMVAL_ACK;<node_id>
    #       e.g.:       SMVAL_ACK;2
    'smsg_type': 'SMVAL_ACK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER]
}

SMSG_SETMTRVAL_NACK_DEFN = {
    #       format:     SMVAL_NACK;<node_id>
    #       e.g.:       SMVAL_NACK;2
    'smsg_type': 'SMVAL_NACK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER]
}

SMSG_SETMTRINT_DEFN = {
    #  Requests a change of a node's metering interval to the value specified.  The interval is the period in seconds at which meter entries are created
    #  i.e. (resolution).
    #       format:     SMINT;<node_id>,<new_meter_interval>
    #       e.g.:       SMINT;2,10
    'smsg_type': 'SMINT',
    'smsg_direction': MessageDirection.SVR_TO_GW,
    'smsg_attrib_layout': ['smsg_type', 'node_id', 'new_meter_interval'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER, A_HEADER]
}

SMSG_SETMTRINT_ACK_DEFN = {
    #       format:     SMINT_ACK;<node_id>
    #       e.g.:       SMINT_ACK;2
    'smsg_type': 'SMINT_ACK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER]
}

SMSG_SETMTRINT_NACK_DEFN = {
    #       format:     SMINT_NACK;<node_id>
    #       e.g.:       SMINT_NACK;2
    'smsg_type': 'SMINT_NACK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER]
}

SMSG_SETPLED_DEFN = {
    #  Requests a change of a node's puck LED rate and time.
    #       format:     SPLED;<node_id>,<new_puck_led_rate>,<new_puck_led_time>
    #       e.g.:       SPLED;2,1,100
    'smsg_type': 'SPLED',
    'smsg_direction': MessageDirection.SVR_TO_GW,
    'smsg_attrib_layout': ['smsg_type', 'node_id', 'new_puck_led_rate', 'new_puck_led_time'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER, A_HEADER, A_HEADER]
}

SMSG_SETPLED_ACK_DEFN = {
    #       format:     SPLED_ACK;<node_id>
    #       e.g.:       SPLED_ACK;2
    'smsg_type': 'SPLED_ACK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER]
}

SMSG_SETPLED_NACK_DEFN = {
    #       format:     SPLED_NACK;<node_id>
    #       e.g.:       SPLED_NACK;2
    'smsg_type': 'SPLED_NACK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER]
}

SMSG_NODEDARK_DEFN = {
    #       format:     NDARK;<node_id>,<last_seen>
    #       e.g.:       NDARK;2,1496842913428
    'smsg_type': 'NDARK',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id', 'last_seen'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER, A_HEADER]
}

SMSG_GPMSG_DEFN = {
    #       format:     GMSG;<node_id>,GMSG,<message>
    #       e.g.:       GMSG;2,GMSG,message
    'smsg_type': 'GMSG',
    'rmsg_type': 'GMSG',
    'smsg_direction': MessageDirection.GW_TO_SVR,
    'smsg_attrib_layout': ['smsg_type', 'node_id', 'rmsg_type', 'message'],
    'smsg_attrib_type': [A_HEADER_SKIP, A_HEADER, A_HEADER_SKIP, A_HEADER]
}

# ==============================================================================================================================================================
#  IMPLEMENTATION
# ==============================================================================================================================================================

# ----------------------------------------------------------------------------------------------------------------------------------------------------------
#  Get messages in serial format
# ----------------------------------------------------------------------------------------------------------------------------------------------------------

def get_server_time_msg():
    return get_message_str(SMSG_GETTIME_DEFN)


def set_gateway_time_msg(new_epoch_time_utc):
    return get_message_str(SMSG_SETTIME_DEFN, header={'new_epoch_time_utc': new_epoch_time_utc})


def set_gateway_time_ack_msg():
    return get_message_str(SMSG_SETTIME_ACK_DEFN)


def set_gateway_time_nack_msg():
    return get_message_str(SMSG_SETTIME_NACK_DEFN)


def get_gateway_snapshot_msg():
    return get_message_str(SMSG_GETGWSNAP_DEFN)


def gateway_snapshot_msg(gateway_id, when_booted, free_ram, time, log_level, encrypt_key, network_id, tx_power):
    return get_message_str(SMSG_GWSNAP_DEFN, header={'gateway_id': gateway_id, 'when_booted': when_booted, 'free_ram': free_ram, 'time': time,
                'log_level': log_level, 'encrypt_key': encrypt_key, 'network_id': network_id, 'tx_power': tx_power})


def set_gw_inst_tmp_rate_msg(node_id, tmp_poll_rate, tmp_poll_period):
    return get_message_str(SMSG_SETGITR_DEFN, header={'node_id': node_id, 'tmp_poll_rate': tmp_poll_rate, 'tmp_poll_period': tmp_poll_period})


def set_gateway_inst_tmp_rate_ack_msg():
    return get_message_str(SMSG_SETGITR_ACK_DEFN)


def set_gateway_inst_tmp_rate_nack_msg():
    return get_message_str(SMSG_SETGITR_NACK_DEFN)


def get_node_snapshot_msg(node_id):
    return get_message_str(SMSG_GETNODESNAP_DEFN, header={'node_id': node_id})


def node_snapshot_msg(node_id, batt_voltage, up_time,sleep_time, free_ram, last_seen, last_clock_drift, meter_interval, meter_impulses_per_kwh, last_meter_entry_finish,
                      last_meter_value, puck_led_rate, puck_led_time,last_rssi_at_gateway):
    return get_message_str(SMSG_NODESNAP_DEFN, detail_recs=[{'node_id': node_id, 'batt_voltage': batt_voltage, 'up_time': up_time, 'sleep_time': sleep_time,
                             'free_ram': free_ram, 'last_seen': last_seen, 'last_clock_drift': last_clock_drift, 'meter_interval': meter_interval, 'meter_impulses_per_kwh': meter_impulses_per_kwh,
                             'last_meter_entry_finish': last_meter_entry_finish, 'last_meter_value': last_meter_value, 'puck_led_rate': puck_led_rate,
                             'puck_led_time': puck_led_time, 'last_rssi_at_gateway': last_rssi_at_gateway}])


def get_node_snapshot_nack_msg():
    return get_message_str(SMSG_GETNODESNAP_NACK_DEFN)


def meter_update_no_irms_msg(node_id, start_timestamp, start_meter_value, meter_entries):
    # meter entries is list of detail record tuples
    entry_list = []

    for entry in meter_entries:
            entry_list.append({'entry_interval_len': entry.entry_interval_len, 'entry_value': entry.entry_value})

    return get_message_str(SMSG_MTRUPDATE_NO_IRMS_DEFN, header={'node_id': node_id, 'start_timestamp': start_timestamp, 'start_meter_value': start_meter_value}, detail_recs=entry_list)


def meter_update_with_irms_msg(node_id, start_timestamp, start_meter_value, meter_entries):
    # meter entries is list of detail record tuples
    entry_list = []

    for entry in meter_entries:
            entry_list.append({'entry_interval_len': entry.entry_interval_len, 'entry_value': entry.entry_value, 'spot_rms_current': entry.spot_rms_current})

    return get_message_str(SMSG_MTRUPDATE_WITH_IRMS_DEFN, header={'node_id': node_id, 'start_timestamp': start_timestamp, 'start_meter_value': start_meter_value}, detail_recs=entry_list)


def meter_rebase_msg(node_id, entry_timestamp, meter_value):
    return get_message_str(SMSG_MTRREBASE_DEFN, header={'node_id': node_id, 'entry_timestamp': entry_timestamp, 'meter_value': meter_value})


def set_node_meter_value_msg(node_id, new_meter_value):
    return get_message_str(SMSG_SETMTRVAL_DEFN, header={'node_id': node_id, 'new_meter_value': new_meter_value})


def set_node_meter_value_ack_msg():
    return get_message_str(SMSG_SETMTRVAL_ACK_DEFN)


def set_node_meter_value_nack_msg():
    return get_message_str(SMSG_SETMTRVAL_NACK_DEFN)


def set_node_meter_interval_msg(node_id, new_meter_interval):
    return get_message_str(SMSG_SETMTRINT_DEFN, header={'node_id': node_id, 'new_meter_interval': new_meter_interval})


def set_node_meter_interval_ack_msg():
    return get_message_str(SMSG_SETMTRINT_ACK_DEFN)


def set_node_meter_interval_nack_msg():
    return get_message_str(SMSG_SETMTRINT_NACK_DEFN)


def set_node_puck_led_msg(node_id, new_puck_led_rate, new_puck_led_time):
    return get_message_str(SMSG_SETPLED_DEFN, header={'node_id': node_id, 'new_puck_led_rate': new_puck_led_rate, 'new_puck_led_time': new_puck_led_time})


def set_node_puck_led_ack_msg():
    return get_message_str(SMSG_SETPLED_ACK_DEFN)


def set_node_puck_led_nack_msg():
    return get_message_str(SMSG_SETPLED_NACK_DEFN)


def meter_node_dark_msg(node_id, last_seen):
    return get_message_str(SMSG_NODEDARK_DEFN, header={'node_id': node_id, 'last_seen': last_seen})


def general_purpose_msg(node_id, message):
    return get_message_str(SMSG_GPMSG_DEFN, header={'node_id': node_id, 'message': message})


def get_random_meter_update_msg(node_id, start_time=None, start_value=0, interval=15, entry_min=1, entry_max=10):

    if start_time is None:
        start_time = arrow.utcnow().shift(seconds=(-7 * interval)).timestamp

    if randint(0, 30) == 15:
        return meter_rebase_msg(node_id, start_time, start_value)
    else:
        meter_entries = []
        num_elements = randint(1, 7)
        for i in range (1, num_elements):
            entry_value = randint(entry_min, entry_max)
            meter_entries.append({'entry_interval_len': interval, 'entry_value': entry_value})
        return meter_update_no_irms_msg(node_id, start_time, start_value, meter_entries)


def get_random_gateway_event_msg(node_id):

    msg_selection = randint(0, 4)

    if msg_selection == 0:
        return get_server_time_msg()
    if msg_selection == 1:
        return gateway_snapshot_msg(1, arrow.utcnow().shift(hours=-1).timestamp, 500, arrow.utcnow().timestamp, 'DEBUG', 'CHANGE_ME_PLEASE', '0.0.1.1', -10)
    if msg_selection == 2:
        return node_snapshot_msg(node_id, 4000, 10000, 9000, 700, arrow.utcnow().shift(minutes=-1).timestamp, 5, 15, 1000, arrow.utcnow().shift(minutes=-2).timestamp,
                                 50000, 1, 0, -60)
    if msg_selection == 3:
        return meter_node_dark_msg(node_id, arrow.utcnow().shift(minutes=-5).timestamp)
    if msg_selection == 4:
        return general_purpose_msg(node_id, 'HELLO!!!')

# ----------------------------------------------------------------------------------------------------------------------------------------------------------
#  MESSAGE / MSG. OBJECT FUNCTIONS
# ----------------------------------------------------------------------------------------------------------------------------------------------------------

def register_message_defn(message_defn):
    '''
    Adds message definition to dict of definitions, with namedtuples for object header and detail items.
    '''

    tmp_header_defn = []
    tmp_detail_defn = []

    for i, attrib_type in enumerate(message_defn['smsg_attrib_type']):
        if attrib_type == A_HEADER:
            tmp_header_defn.append(message_defn['smsg_attrib_layout'][i])
        elif attrib_type == A_DETAIL:
            tmp_detail_defn.append(message_defn['smsg_attrib_layout'][i])

    message_defn['obj_header_defn'] = namedtuple('Header', tmp_header_defn)
    message_defn['obj_detail_defn'] = namedtuple('Detail', tmp_detail_defn)

    message_definitions[message_defn['smsg_type']] = message_defn


def get_message_str(message_defn, header=None, detail_recs=None):
    '''
    Returns string representation of message object given dict of header, list of dicts of details.
    '''
    message_str = message_defn['smsg_type']
    if header is not None:
        message_str += SMSG_RS
        for i, attr_name in enumerate(message_defn['smsg_attrib_layout']):
            if message_defn['smsg_attrib_type'][i].startswith(A_HEADER):
                if i > 1:
                    message_str += SMSG_FS
                if attr_name is 'rmsg_type':
                    message_str += message_defn['rmsg_type']
                elif attr_name is not 'smsg_type':
                    message_str += str(header[attr_name])

    if detail_recs is not None:
        for detail_record in detail_recs:
            message_str += SMSG_RS
            first_detail = True
            for i, attr_name in enumerate(message_defn['smsg_attrib_layout']):
                if message_defn['smsg_attrib_type'][i].startswith(A_DETAIL):
                    if not first_detail:
                        message_str += SMSG_FS
                    else:
                        first_detail = False
                    message_str += str(detail_record[attr_name])

    return message_str


def get_attrib_defn_idx(message_type, msg_attrib_pos):
    '''
    takes absolute message attribute position (could be greater than number of attribs in msg layout where
    detail record repeats) and returns corresponding message layout index.
    '''

    message_defn = message_definitions[message_type]

    if msg_attrib_pos < len(message_defn['smsg_attrib_layout']):
        return msg_attrib_pos
    else:
        # Are in repeating detail record.  Find position in layout by getting first
        # detail attrib position, subtracting that from current message position, then doing
        # modulo of detail record length
        detail_start_pos = message_defn['smsg_attrib_type'].index(A_DETAIL)
        detail_len = len(message_defn['smsg_attrib_type']) - detail_start_pos
        attrib_defn_pos = detail_start_pos + ((msg_attrib_pos - detail_start_pos) % detail_len)

        return attrib_defn_pos


def get_message_obj(message_str, gateway_uuid, gateway_id, network_id):
    when_received = arrow.utcnow().timestamp

    # remove message rx/tx prefix
    message_str = message_str.replace(SMSG_RX_PREFIX, '')

    # split into records (will be at least 2)
    msg_in_records = list(filter(None, message_str.split(SMSG_RS)))

    header_defn = None
    detail_defn = None
    message_defn = None
    transfer_obj = {'header_count': 0, 'detail_count': 0, 'message_type': A_UNKNOWN}  # dict used to pass message to processor
    message_attrib_pos = 0
    message_type = A_UNKNOWN
    current_rec_type = A_UNKNOWN
    header_count = 0
    detail_count = 0

    for rec_pos, msg_in_record in enumerate(msg_in_records):
        record_attribs = msg_in_record.split(SMSG_FS)
        tmp_transfer_obj = {}

        # iterate through each attribute, building up an output object to be passed to
        # the appropriate processor
        for record_attrib_pos, record_attrib_val in enumerate(record_attribs):
            # message type is first (only) attribute in first record
            if message_attrib_pos == 0:
                message_type = record_attrib_val
                # TODO: handle garbled message, partial or no match on message type
                message_defn = message_definitions[message_type]
                header_defn = message_defn['obj_header_defn']
                detail_defn = message_defn['obj_detail_defn']
                transfer_obj['message_type'] = message_type
                transfer_obj['when_received'] = when_received
                transfer_obj['gateway_uuid'] = gateway_uuid
                transfer_obj['gateway_id'] = gateway_id
                transfer_obj['network_id'] = network_id

            else:
                # get position of attribute in message definition
                attrib_defn_pos = get_attrib_defn_idx(message_type, message_attrib_pos)
                attrib_defn_name = message_defn['smsg_attrib_layout'][attrib_defn_pos]
                attrib_defn_type = message_defn['smsg_attrib_type'][attrib_defn_pos]

                if attrib_defn_type not in[A_HEADER_SKIP, A_DETAIL_SKIP]:
                    tmp_transfer_obj[attrib_defn_name] = record_attrib_val

                if attrib_defn_type is not current_rec_type and attrib_defn_type in [A_HEADER, A_HEADER_SKIP]:
                    current_rec_type = A_HEADER
                elif attrib_defn_type is not current_rec_type and attrib_defn_type in [A_DETAIL, A_DETAIL_SKIP]:
                    current_rec_type = A_DETAIL

            message_attrib_pos += 1

        if message_attrib_pos > 1:
            # end of record, add transfer object item (named tuple) to transfer object
            if current_rec_type is A_HEADER:
                header_count += 1
                new_obj_item = header_defn(**tmp_transfer_obj)
            else:
                detail_count += 1
                new_obj_item = detail_defn(**tmp_transfer_obj)

            transfer_obj[current_rec_type + '_' + str(rec_pos)] = new_obj_item

    # make transfer object
    transfer_obj['header_count'] = header_count
    transfer_obj['detail_count'] = detail_count

    return transfer_obj


register_message_defn(SMSG_GETTIME_DEFN)
register_message_defn(SMSG_SETTIME_DEFN)
register_message_defn(SMSG_SETTIME_ACK_DEFN)
register_message_defn(SMSG_SETTIME_NACK_DEFN)
register_message_defn(SMSG_GETGWSNAP_DEFN)
register_message_defn(SMSG_GWSNAP_DEFN)
register_message_defn(SMSG_SETGITR_DEFN)
register_message_defn(SMSG_SETGITR_ACK_DEFN)
register_message_defn(SMSG_SETGITR_NACK_DEFN)
register_message_defn(SMSG_GETNODESNAP_DEFN)
register_message_defn(SMSG_NODESNAP_DEFN)
register_message_defn(SMSG_GETNODESNAP_NACK_DEFN)
register_message_defn(SMSG_MTRUPDATE_NO_IRMS_DEFN)
register_message_defn(SMSG_MTRUPDATE_WITH_IRMS_DEFN)
register_message_defn(SMSG_MTRREBASE_DEFN)
register_message_defn(SMSG_SETMTRVAL_DEFN)
register_message_defn(SMSG_SETMTRVAL_ACK_DEFN)
register_message_defn(SMSG_SETMTRVAL_NACK_DEFN)
register_message_defn(SMSG_SETPLED_DEFN)
register_message_defn(SMSG_SETPLED_ACK_DEFN)
register_message_defn(SMSG_SETPLED_NACK_DEFN)
register_message_defn(SMSG_SETMTRINT_DEFN)
register_message_defn(SMSG_SETMTRINT_ACK_DEFN)
register_message_defn(SMSG_SETMTRINT_NACK_DEFN)
register_message_defn(SMSG_NODEDARK_DEFN)
register_message_defn(SMSG_GPMSG_DEFN)

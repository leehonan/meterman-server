#!flask/bin/python

import ipaddress
import threading
import arrow
from flask import Flask, make_response, jsonify, request
from flask_httpauth import HTTPBasicAuth
from flask_restful import reqparse, Api, Resource
import json
from meterman import meter_db as db, app_base as base, viz_data

MAX_REQ_ITEMS = 100000
DEF_REQ_ITEMS = 100
REQ_WILDCARDS = {'all', '*'}

api_user = ''
api_password = ''
api_access_lan_only = False

app = Flask(__name__)
api = Api(app)
auth = HTTPBasicAuth()
meter_man = None
logger = None

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST')
    return response


def check_access_auth():
    client_ip = ipaddress.ip_address(request.remote_addr)
    local_ip = base.get_ip_address()
    allow_access = True

    if api_access_lan_only:
        allow_access = client_ip in ipaddress.ip_network('127.0.0.1') or client_ip in ipaddress.ip_network(local_ip + '/24', strict=False)

    logger.info('API auth attempt from {}. Local IP is {}. LAN only={}. Allow access={}'.format(client_ip, local_ip, api_access_lan_only, allow_access))
    return allow_access


@auth.get_password
def get_password(username):
    if (username == api_user and check_access_auth()):
        return api_password
    else:
        return None


@auth.error_handler
def unauthorized():
    return make_response(jsonify({'error': 'Unauthorized access'}), 403)


def validate_utc_ts(utc_ts):
    try:
        return base.MIN_TIME <= arrow.get(utc_ts).timestamp <= base.MAX_TIME
    except ValueError:
        return False


class MeterEntries(Resource):
    @auth.login_required
    def get(self, node_uuid):
        if node_uuid.lower() in REQ_WILDCARDS:
            node_uuid = None
        parser = reqparse.RequestParser()
        parser.add_argument('time_from', type=int, help='start time as epoch UTC, default is none')
        parser.add_argument('time_to', type=int, help='finish time as epoch UTC, default is none')
        parser.add_argument('item_limit', type=int, help='number of results, max is {}, default is {}'.format(MAX_REQ_ITEMS, DEF_REQ_ITEMS))
        args = parser.parse_args()

        time_from = args['time_from']
        time_to = args['time_to']
        item_limit = args['item_limit'] if args['item_limit'] is not None else DEF_REQ_ITEMS

        request_valid = True
        request_bad_messages = []

        if time_from is not None and validate_utc_ts(time_from) is False:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_from.  Must be valid UNIX epoch timestamp '
                                        'on or before time_to, and between {0} and {1}.'.format(base.MIN_TIME, base.MAX_TIME)})

        if time_to is not None and (validate_utc_ts(time_to) is False or time_to < time_from):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_to.  Must be valid UNIX epoch timestamp '
                                        'on or after time_from, and between {0} and {1}.'.format(base.MIN_TIME, base.MAX_TIME)})


        if item_limit is not None and not (0 < item_limit <= MAX_REQ_ITEMS):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid item_limit.'})

        if not request_valid:
            return make_response(jsonify({'status': 'Bad Request', 'errors': request_bad_messages}), 400)

        meter_entries = meter_man.data_mgr.get_meter_entries(node_uuid, time_from=time_from, time_to=time_to,
                                                                    limit_count=item_limit)

        if meter_entries is None:
            return jsonify({'request': {'node_uuid': node_uuid, 'item_limit': item_limit, 'time_from': time_from, 'time_to': time_to},
                    'result': {'meter_entries': None}})
        else:
            return jsonify({'request': {'node_uuid': node_uuid, 'item_limit': item_limit, 'time_from': time_from, 'time_to': time_to},
                    'result': {'meter_entries': meter_entries}})

api.add_resource(MeterEntries, '/meterentries/<node_uuid>')


class MeterConsumption(Resource):

    @auth.login_required
    def get(self, node_uuid):
        if node_uuid.lower() in REQ_WILDCARDS:
            node_uuid = None
        parser = reqparse.RequestParser()
        parser.add_argument('time_from', type=int, help='start time as epoch UTC, default is none')
        parser.add_argument('time_to', type=int, help='finish time as epoch UTC, default is none')
        args = parser.parse_args()

        time_from = args['time_from']
        time_to = args['time_to']

        request_valid = True
        request_bad_messages = []

        if time_from is not None and validate_utc_ts(time_from) is False:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_from.  Must be valid UNIX epoch timestamp '
                                        'on or before time_to, and between {0} and {1}.'.format(base.MIN_TIME, base.MAX_TIME)})

        if time_to is not None and (validate_utc_ts(time_to) is False or time_to < time_from):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_to.  Must be valid UNIX epoch timestamp '
                                        'on or after time_from, and between {0} and {1}.'.format(base.MIN_TIME, base.MAX_TIME)})

        if node_uuid is None:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Node UUID required.'})

        if not request_valid:
            return make_response(jsonify({'status': 'Bad Request', 'errors': request_bad_messages}), 400)

        mc = meter_man.data_mgr.get_meter_consumption(node_uuid, time_from=time_from, time_to=time_to)

        logger.debug('Got consumption request for node {} from {} to {}.  Returned {} Wh, with calc breakdown... {}'.format(node_uuid, time_from, time_to,
                                                                                                                            mc['meter_consumption'],
                                                                                                                            mc['calc_breakdown']))

        if mc is None:
            return jsonify({'request': {'node_uuid': node_uuid, 'time_from': time_from, 'time_to': time_to},
                    'result': {'meter_consumption': None}})
        else:
            return jsonify({'request': {'node_uuid': node_uuid, 'time_from': time_from, 'time_to': time_to},
                    'result': mc})

api.add_resource(MeterConsumption, '/meterconsumption/<node_uuid>')


class GatewaySnapshots(Resource):
    @auth.login_required
    def get(self, gateway_uuid):
        if gateway_uuid.lower() in REQ_WILDCARDS:
            gateway_uuid = None
        parser = reqparse.RequestParser()
        parser.add_argument('time_from', type=int, help='start time as epoch UTC, default is none')
        parser.add_argument('time_to', type=int, help='finish time as epoch UTC, default is none')
        parser.add_argument('item_limit', type=int, help='number of results, max is {}, default is {}'.format(MAX_REQ_ITEMS, DEF_REQ_ITEMS))
        args = parser.parse_args()

        time_from = args['time_from']
        time_to = args['time_to']
        item_limit = args['item_limit'] if args['item_limit'] is not None else DEF_REQ_ITEMS

        request_valid = True
        request_bad_messages = []

        if time_from is not None and validate_utc_ts(time_from) is False:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_from.  Must be valid UNIX epoch timestamp '
                                        'on or before time_to, and between {0} and {1}.'.format(base.MIN_TIME, base.MAX_TIME)})

        if time_to is not None and (validate_utc_ts(time_to) is False or time_to < time_from):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_to.  Must be valid UNIX epoch timestamp '
                                        'on or after time_from, and between {0} and {1}.'.format(base.MIN_TIME, base.MAX_TIME)})


        if item_limit is not None and not (0 < item_limit <= MAX_REQ_ITEMS):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid item_limit.'})

        if not request_valid:
            return make_response(jsonify({'status': 'Bad Request', 'errors': request_bad_messages}), 400)

        gateway_snapshots = meter_man.data_mgr.get_gw_snapshots(gateway_uuid, time_from=time_from, time_to=time_to,
                                                                    limit_count=item_limit)

        if gateway_snapshots is None:
            return jsonify({'request': {'gateway_uuid': gateway_uuid, 'item_limit': item_limit, 'time_from': time_from, 'time_to': time_to},
                    'result': {'gateway_snapshots': None}})
        else:
            return jsonify({'request': {'gateway_uuid': gateway_uuid, 'item_limit': item_limit, 'time_from': time_from, 'time_to': time_to},
                    'result': {'gateway_snapshots': gateway_snapshots}})

api.add_resource(GatewaySnapshots, '/gatewaysnapshots/<gateway_uuid>')


class NodeSnapshots(Resource):
    @auth.login_required
    def get(self, node_uuid):
        if node_uuid.lower() in REQ_WILDCARDS:
            node_uuid = None
        parser = reqparse.RequestParser()
        parser.add_argument('time_from', type=int, help='start time as epoch UTC, default is none')
        parser.add_argument('time_to', type=int, help='finish time as epoch UTC, default is none')
        parser.add_argument('item_limit', type=int, help='number of results, max is {}, default is {}'.format(MAX_REQ_ITEMS, DEF_REQ_ITEMS))
        args = parser.parse_args()

        time_from = args['time_from']
        time_to = args['time_to']
        item_limit = args['item_limit'] if args['item_limit'] is not None else DEF_REQ_ITEMS

        request_valid = True
        request_bad_messages = []

        if time_from is not None and validate_utc_ts(time_from) is False:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_from.  Must be valid UNIX epoch timestamp '
                                        'on or before time_to, and between {0} and {1}.'.format(base.MIN_TIME, base.MAX_TIME)})

        if time_to is not None and (validate_utc_ts(time_to) is False or time_to < time_from):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_to.  Must be valid UNIX epoch timestamp '
                                        'on or after time_from, and between {0} and {1}.'.format(base.MIN_TIME, base.MAX_TIME)})


        if item_limit is not None and not (0 < item_limit <= MAX_REQ_ITEMS):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid item_limit.'})

        if not request_valid:
            return make_response(jsonify({'status': 'Bad Request', 'errors': request_bad_messages}), 400)

        node_snapshots = meter_man.data_mgr.get_node_snapshots(node_uuid, time_from=time_from, time_to=time_to,
                                                                    limit_count=item_limit)

        if node_snapshots is None:
            return jsonify({'request': {'node_uuid': node_uuid, 'item_limit': item_limit, 'time_from': time_from, 'time_to': time_to},
                    'result': {'node_snapshots': None}})
        else:
            return jsonify({'request': {'node_uuid': node_uuid, 'item_limit': item_limit, 'time_from': time_from, 'time_to': time_to},
                    'result': {'node_snapshots': node_snapshots}})

api.add_resource(NodeSnapshots, '/nodesnapshots/<node_uuid>')


class NodeEvents(Resource):
    @auth.login_required
    def get(self, node_uuid):
        if node_uuid.lower() in REQ_WILDCARDS:
            node_uuid = None
        parser = reqparse.RequestParser()
        parser.add_argument('time_from', type=int, help='start time as epoch UTC, default is none')
        parser.add_argument('time_to', type=int, help='finish time as epoch UTC, default is none')
        parser.add_argument('item_limit', type=int, help='number of results, max is {}, default is {}'.format(MAX_REQ_ITEMS, DEF_REQ_ITEMS))
        args = parser.parse_args()

        time_from = args['time_from']
        time_to = args['time_to']
        item_limit = args['item_limit'] if args['item_limit'] is not None else DEF_REQ_ITEMS

        request_valid = True
        request_bad_messages = []

        if time_from is not None and validate_utc_ts(time_from) is False:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_from.  Must be valid UNIX epoch timestamp '
                                        'on or before time_to, and between {0} and {1}.'.format(base.MIN_TIME, base.MAX_TIME)})

        if time_to is not None and (validate_utc_ts(time_to) is False or time_to < time_from):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_to.  Must be valid UNIX epoch timestamp '
                                        'on or after time_from, and between {0} and {1}.'.format(base.MIN_TIME, base.MAX_TIME)})


        if item_limit is not None and not (0 < item_limit <= MAX_REQ_ITEMS):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid item_limit.'})

        if not request_valid:
            return make_response(jsonify({'status': 'Bad Request', 'errors': request_bad_messages}), 400)

        node_events = meter_man.data_mgr.get_node_events(node_uuid, time_from=time_from, time_to=time_to,
                                                                    limit_count=item_limit)

        if node_events is None:
            return jsonify({'request': {'node_uuid': node_uuid, 'item_limit': item_limit, 'time_from': time_from, 'time_to': time_to},
                    'result': {'node_events': None}})
        else:
            return jsonify({'request': {'node_uuid': node_uuid, 'item_limit': item_limit, 'time_from': time_from, 'time_to': time_to},
                    'result': {'node_events': node_events}})

api.add_resource(NodeEvents, '/nodeevents/<node_uuid>')


class NodeCtrl(Resource):
    @auth.login_required
    def put(self, node_uuid):
        parser = reqparse.RequestParser()
        parser.add_argument('tmp_ginr_poll_rate', type=int,
                            help='temporary aggressive GINR rate to ensure new settings applied quickly')
        parser.add_argument('tmp_ginr_poll_time', type=int,
                            help='duration of temporary aggressive GINR polling, default is 300')
        parser.add_argument('meter_value', type=int, help='meter value in Wh, default is none')
        parser.add_argument('meter_interval', type=int, help='meter interval in seconds, default is none')
        parser.add_argument('puck_led_rate', type=int, help='LED blink ratio to watched LED pulses, as 1:x')
        parser.add_argument('puck_led_time', type=int, help='blink duration in ms, 0 means same as watched LED pulse')
        args = parser.parse_args()

        tmp_ginr_poll_rate = args['tmp_ginr_poll_rate']
        tmp_ginr_poll_time = args['tmp_ginr_poll_time']
        meter_value = args['meter_value']
        meter_interval = args['meter_interval']
        puck_led_rate = args['puck_led_rate']
        puck_led_time = args['puck_led_time']
        input_arg_count = (sum(x is not None for x in [tmp_ginr_poll_rate, meter_value,
                                                       meter_interval, puck_led_rate]))

        request_valid = True
        request_bad_messages = []

        if input_arg_count > 1:
            request_valid = False
            request_bad_messages.append(
                {'api_error': 'Invalid request', 'message': 'Invalid arguments - can only request one GINR poll rate/time, meter value, meter interval, '
                                                            'or LED rate/time per request'})

        if tmp_ginr_poll_rate is not None and tmp_ginr_poll_time is None:
            tmp_ginr_poll_time = 300

        if tmp_ginr_poll_rate is not None and tmp_ginr_poll_rate < 10 or tmp_ginr_poll_rate > 600:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid GINR Poll rate.  Must be between 10 and 600.'})

        if tmp_ginr_poll_time is not None and tmp_ginr_poll_time < 10 or tmp_ginr_poll_time > 3000:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid GINR Poll time.  Must be between 10 and 3000.'})

        if (puck_led_rate is not None or puck_led_time is not None) and (puck_led_rate is None or puck_led_time is None):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Puck LED rate AND time must be specified'})

        if puck_led_rate is not None and puck_led_rate > 255:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid LED rate.  Must be between 0 and 255.'})

        if puck_led_time is not None and puck_led_time > 3000:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid LED time.  Must be between 0 and 3000ms.'})


        if not request_valid:
            return make_response(jsonify({'status': 'Bad Request', 'errors': request_bad_messages}), 400)

        if tmp_ginr_poll_rate is not None and tmp_ginr_poll_time is not None:
            meter_man.device_mgr.set_node_gw_inst_tmp_rate(node_uuid, tmp_ginr_poll_rate, tmp_ginr_poll_time)
        elif meter_value is not None:
            meter_man.device_mgr.set_node_meter_value(node_uuid, meter_value)
        elif meter_interval is not None:
            meter_man.device_mgr.set_node_meter_interval(node_uuid, meter_interval)
        elif puck_led_rate is not None and puck_led_time is not None:
            meter_man.device_mgr.set_node_puck_led(node_uuid, puck_led_rate, puck_led_time)

        return jsonify({'request': {'tmp_ginr_poll_rate': tmp_ginr_poll_rate, 'tmp_ginr_poll_time': tmp_ginr_poll_time,
                                    'meter_value': meter_value, 'meter_interval': meter_interval, 'puck_led_rate': puck_led_rate,
                                    'puck_led_time': puck_led_time}, 'result': 'request queued.'})

api.add_resource(NodeCtrl, '/nodectrl/<node_uuid>')


class MeterDataDelete(Resource):
    @auth.login_required
    def put(self, node_uuid):
        if node_uuid.lower() in REQ_WILDCARDS:
            node_uuid = None

        parser = reqparse.RequestParser()
        parser.add_argument('time_from', type=int, help='start time as epoch UTC, mandatory')
        parser.add_argument('time_to', type=int, help='finish time as epoch UTC, mandatory')
        parser.add_argument('entry_type', type=str, help='Must be provided, one of: all, update, rebase, synth-update, synth-rebase, synth-all')
        args = parser.parse_args()

        time_from = args['time_from']
        time_to = args['time_to']
        entry_type = args['entry_type']

        request_valid = True
        request_bad_messages = []

        if entry_type is None or entry_type.lower() not in ['all', 'update', 'rebase', 'synth-update', 'synth-rebase', 'synth-all']:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid entry type.  Must be provided, one of: all, update, rebase, synth-update, synth-rebase, synth-all.'})

        if time_from is None or (time_from is not None and validate_utc_ts(time_from) is False):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_from.  Must be valid UNIX epoch timestamp '
                                                                                    'on or before time_to, and between {0} and {1}.'.format(base.MIN_TIME,
                                                                                                                                            base.MAX_TIME)})

        if time_to is None or (time_to is not None and (validate_utc_ts(time_to) is False or time_to < time_from)):
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Invalid time_to.  Must be valid UNIX epoch timestamp '
                                                                                    'on or after time_from, and between {0} and {1}.'.format(base.MIN_TIME,
                                                                                                                                             base.MAX_TIME)})

        if not request_valid:
            return make_response(jsonify({'status': 'Bad Request', 'errors': request_bad_messages}), 400)

        del_entry_types = []
        if entry_type == 'all':
            del_entry_types.append(db.EntryType.METER_UPDATE)
            del_entry_types.append(db.EntryType.METER_REBASE)
            del_entry_types.append(db.EntryType.METER_UPDATE_SYNTH)
            del_entry_types.append(db.EntryType.METER_UPDATE_SYNTH)
        elif entry_type == 'synth-all':
            del_entry_types.append(db.EntryType.METER_UPDATE_SYNTH)
            del_entry_types.append(db.EntryType.METER_UPDATE_SYNTH)
        elif entry_type == 'update':
            del_entry_types.append(db.EntryType.METER_UPDATE)
        elif entry_type == 'rebase':
            del_entry_types.append(db.EntryType.METER_REBASE)
        elif entry_type == 'synth-update':
            del_entry_types.append(db.EntryType.METER_UPDATE_SYNTH)
        elif entry_type == 'synth-rebase':
            del_entry_types.append(db.EntryType.METER_REBASE_SYNTH)

        for del_entry_type in del_entry_types:
            meter_man.data_mgr.delete_meter_entries_in_range(node_uuid=node_uuid, time_from=time_from, time_to=time_to, entry_type=del_entry_type)

        return jsonify({'request': {'node_uuid': node_uuid, 'time_from': time_from, 'time_to': time_to, 'entry_type': entry_type},
                        'result': {'operation_delete': 'OK.  *Marked* as deleted in DB.'}})

api.add_resource(MeterDataDelete, '/meterdata/delete/<node_uuid>')


class MeterDataUpload(Resource):
    # Allows upoad of a block of csv or json interval entries, or the generation of entries with a given interval and value from a start time.
    # CSV record format is "<when_start(utc_epoch)>,<entry_value>,<entry_interval_length>,<meter_value>;", json is "{'when_start':'<time(utc_epoch)>','entry_value':'<entry_value>',
    # 'entry_interval_length':'<entry_interval_length>,'meter_value':'<meter_value>'}"
    # Reads created as synthetic mups.  Will force creation of synthetic rebase.  time from and time to will be used to mark prior entries in range as deleted.
    @auth.login_required
    def put(self, node_uuid, operation):
        if node_uuid.lower() in REQ_WILDCARDS:
            node_uuid = None

        request_valid = True
        request_bad_messages = []

        if operation is None or operation.lower() not in ['csv-reads', 'json-reads', 'generator']:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request',
                                         'message': 'Invalid operation.  Must be provided, one of: csv-reads, json-reads, generator'})

        parser = reqparse.RequestParser()
        parser.add_argument('time_from', type=int, help='start time as epoch UTC, mandatory')
        parser.add_argument('time_to', type=int, help='finish time as epoch UTC, mandatory')
        parser.add_argument('gen_start_meter_value', type=int, help='generator starting accum meter val')
        parser.add_argument('gen_entry_value', type=int, help='generator entries to be this size in Wh')
        parser.add_argument('gen_interval_length', type=int, help='generator interval length')
        parser.add_argument('gen_entry_count', type=int, help='generator entry count')
        parser.add_argument('meter_data', type=str,
                            help='Meter data in CSV or JSON format.')
        parser.add_argument('lift_later_reads', type=bool,
                            help='Whether to lift later reads.')

        args = parser.parse_args()

        time_from = args['time_from']
        time_to = args['time_to']
        gen_start_meter_value = args['gen_start_meter_value']
        gen_entry_value = args['gen_entry_value']
        gen_interval_length = args['gen_interval_length']
        gen_entry_count = args['gen_entry_count']
        meter_data = args['meter_data']
        lift_later_reads = args['lift_later_reads']

        meter_entries = []

        if time_from is None or (time_from is not None and validate_utc_ts(time_from) is False):
            request_valid = False
            request_bad_messages.append(
                {'api_error': 'Invalid request', 'message': 'Invalid time_from.  Must be valid UNIX epoch timestamp '
                                                            'on or before time_to, and between {0} and {1}.'.format(
                    base.MIN_TIME,
                    base.MAX_TIME)})

        if time_to is None or (time_to is not None and (validate_utc_ts(time_to) is False or time_to < time_from)):
            request_valid = False
            request_bad_messages.append(
                {'api_error': 'Invalid request', 'message': 'Invalid time_to.  Must be valid UNIX epoch timestamp '
                                                            'on or after time_from, and between {0} and {1}.'.format(
                    base.MIN_TIME,
                    base.MAX_TIME)})

        if request_valid and operation.lower() in ['json-reads', 'csv-reads'] and meter_data is None:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request',
                                         'message': 'No meter data.'})

        if request_valid and operation.lower() == 'json-reads' and meter_data is not None:
            try:
                json_meter_data = json.loads(meter_data)
            except ValueError as err:
                request_valid = False
                request_bad_messages.append({'api_error': 'Invalid request',
                                             'message': 'Invalid JSON: {}'.format(err)})
            else:
                for entry in json_meter_data:
                    meter_entries.append({'when_start': entry['when_start'], 'entry_value': entry['entry_value'],
                                          'entry_interval_length': entry['entry_interval_length'],
                                          'meter_value': entry['meter_value']})

        elif request_valid and operation.lower() == 'csv-reads' and meter_data is not None:
            for entry in meter_data.split(';'):
                entry_elements = entry.split(',')
                meter_entries.append({'when_start': int(entry_elements[0]), 'entry_value': int(entry_elements[1]),
                                      'entry_interval_length': int(entry_elements[2]),
                                      'meter_value': int(entry_elements[3])})

        elif request_valid and operation.lower() == 'generator':
            if gen_entry_value is None or gen_interval_length is None or gen_entry_count is None or gen_start_meter_value is None:
                request_valid = False
                request_bad_messages.append({'api_error': 'Invalid request',
                                             'message': 'Must provide gen_entry_value, gen_interval_length, gen_start_meter_value,'
                                                        'or gen_entry_count for generator operation.'})
            else:
                entry_when_start = time_from
                entry_meter_value = gen_start_meter_value
                for i in range(gen_entry_count):
                    meter_entries.append({'when_start': entry_when_start, 'entry_value': gen_entry_value,
                                          'entry_interval_length': gen_interval_length,
                                          'meter_value': entry_meter_value})
                    entry_when_start += gen_interval_length
                    entry_meter_value += gen_entry_value

        if not request_valid:
            return make_response(jsonify({'status': 'Bad Request', 'errors': request_bad_messages}), 400)

        meter_man.data_mgr.upsert_synth_meter_updates(node_uuid=node_uuid, overwrite_time_from=time_from, overwrite_time_to=time_to, meter_entries=meter_entries,
                                                            rebase_first=True, lift_later=lift_later_reads)

        return jsonify(
            {'request': {'operation': operation.lower(), 'node_uuid': node_uuid, 'time_from': time_from, 'time_to': time_to,
                         'gen_start_meter_value':gen_start_meter_value, 'gen_entry_value':gen_entry_value,
                         'gen_interval_length': gen_interval_length, 'gen_entry_count':gen_entry_count,
                         'lift_later_reads':lift_later_reads},
             'result': {operation.lower(): 'OK.  Data uploaded and prior reads in range marked as deleted.'}})

api.add_resource(MeterDataUpload, '/meterdata/upload/<operation>/<node_uuid>')


class MeterDataPlotter(Resource):
    @auth.login_required
    def get(self, node_uuid):
        if node_uuid.lower() in REQ_WILDCARDS:
            node_uuid = None

        parser = reqparse.RequestParser()
        parser.add_argument('output_path', type=str, help='path for plot')
        args = parser.parse_args()

        output_path = args['output_path']

        request_valid = True
        request_bad_messages = []

        try:
            open(output_path, 'w')
        except TypeError or OSError as err:
            request_valid = False
            request_bad_messages.append({'api_error': 'Invalid request', 'message': 'Valid output path must be supplied.'})

        if request_valid:
            try:
                viz_data.output_plot(node_uuid=node_uuid, plot_output_file=output_path, data_mgr=meter_man.data_mgr, db_file=None)
            except ValueError as err:
                logger.warn('Invalid meter data plot request: {0}'.format(err))

        if not request_valid:
            return make_response(jsonify({'status': 'Bad Request', 'errors': request_bad_messages}), 400)
        else:
            return jsonify({'request': {'node_uuid': node_uuid, 'output_path': output_path},
                        'result': {'meter data plot':'Plot (html) written to {}'.format(output_path)}})

api.add_resource(MeterDataPlotter, '/meterdata/plot/<node_uuid>')


class ApiCtrl:

    def __init__(self, meter_man_obj, port=8000, user='rest_user', password='change_me_please', lan_only=False, log_file=base.log_file):
        global meter_man, api_user, api_password, api_access_lan_only, logger
        meter_man = meter_man_obj
        api_user = user
        api_password = password
        api_access_lan_only = lan_only
        logger = base.get_logger(logger_name='api', log_file=log_file)

        self.port = port
        self.run_thread = None

    def run(self):
        logger.info('Starting API implementation server on port {} with lan_only={}...'.format(self.port, api_access_lan_only))
        self.run_thread = threading.Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': self.port, 'debug': False})
        self.run_thread.daemon = True  # Daemonize thread
        self.run_thread.start()  # Start the execution





#!flask/bin/python

import ipaddress
import threading

import arrow
from flask import Flask, make_response, jsonify, request
from flask_httpauth import HTTPBasicAuth
from flask_restful import reqparse, Api, Resource

from meterman import app_base as base

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

        meter_consumption = meter_man.data_mgr.get_meter_consumption(node_uuid, time_from=time_from, time_to=time_to)

        if meter_consumption is None:
            return jsonify({'request': {'node_uuid': node_uuid, 'time_from': time_from, 'time_to': time_to},
                    'result': {'meter_consumption': None}})
        else:
            return jsonify({'request': {'node_uuid': node_uuid, 'time_from': time_from, 'time_to': time_to},
                    'result': {'meter_consumption': meter_consumption}})

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





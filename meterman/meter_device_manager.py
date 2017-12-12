'''

================================================================================================================================================================
meter_device_manager.py
=====================


TODO: remove assumption of one gateway only, every node as child of that
TODO: allow for change of gateway address while running - currently binds by serial port and continues with old address until restart
================================================================================================================================================================

'''

from meterman import app_base as base
import arrow
from meterman import gateway_messages as gmsg
from meterman import meter_device_gateway as gway
from random import randint

NODE_UPDATE_INTERVAL_SECS = 900
GATEWAY_TIME_SYNC_INTERVAL_SECS = 600

class MeterDeviceManager:

    def __init__(self, meter_man, gateway_config_oride=None):
        self.logger = base.get_logger(logger_name='device_mgr')

        self.gateways = {}

        if gateway_config_oride:
            # used for testing, single gateway
            gateway_configs = [gateway_config_oride]
        else:
            gateway_configs = [x for x in base.config.sections() if x.startswith('Gateway')]

        for gateway in gateway_configs:
            if gateway_config_oride:
                gw_config = gateway
            else:
                gw_config = base.config[gateway]
            gateway_uuid = self.get_node_uuid(gw_config['network_id'], gw_config['gateway_id'])
            self.gateways[gateway_uuid] = {}                # TODO: make below attr of gateway object?
            self.gateways[gateway_uuid]['gw_obj'] = gway.MeterDeviceGateway(
                self, network_id=gw_config['network_id'], gateway_id=gw_config['gateway_id'],
                label=gw_config['label'],
                serial_port=gw_config['serial_port'], serial_baud=gw_config['serial_baud'])
            self.gateways[gateway_uuid]['last_rx_msg_obj'] = ''
            self.gateways[gateway_uuid]['last_snap_time'] = 0
            self.gateways[gateway_uuid]['last_clock_sync_time'] = 0
            self.gateways[gateway_uuid]['sim_meters'] = {}

        self.meters = {}

        self.meter_man = meter_man

        meter_sim_configs = [x for x in base.config.sections() if x.startswith('SimMeter')]
        for meter_sim in meter_sim_configs:
            meter_sim_config = base.config[meter_sim]
            self.add_meter_sim(meter_sim_config['network_id'], meter_sim_config['gateway_id'], meter_sim_config['node_id'],
                               meter_sim_config['interval'], meter_sim_config['start_val'], meter_sim_config['read_min'],
                               meter_sim_config['read_max'], meter_sim_config['max_msg_entries'])


    def get_node_uuid(self, network_id, node_id):
        return network_id + '.' + node_id


    def ensure_node_exists(self, node_uuid, node_id, gateway_uuid):
        if node_uuid not in self.meters:
            self.meters[node_uuid] = {}
            self.meters[node_uuid]['node_id'] = node_id
            self.meters[node_uuid]['gateway_uuid'] = gateway_uuid
            self.meter_man.register_node(node_uuid)


    def uts_to_str(self, utc_timestamp):
        return arrow.get(utc_timestamp).to('local').format('YYYY-MM-DD HH:mm:ss')

    def proc_meter_update(self, msg_obj, is_irms):
        node_id = msg_obj['HEADER_1'].node_id
        when_start = int(msg_obj['HEADER_1'].last_entry_finish_time) + 1   # convert to start time
        meter_value = int(msg_obj['HEADER_1'].last_entry_meter_value)

        node_uuid = self.get_node_uuid(msg_obj['network_id'], node_id)
        self.ensure_node_exists(node_uuid, node_id, msg_obj['gateway_uuid'])

        meter_entries_in = {key: value for key, value in msg_obj.items() if key.startswith(gmsg.A_DETAIL)}
        meter_entries_out = []

        if len(meter_entries_in) == 0:
            self.logger.info("Got empty meter update from node " + node_uuid)
            return

        for key, entry in meter_entries_in.items():
            entry_out = entry._asdict()
            when_start += int(entry.entry_interval_length)
            entry_out['when_start'] = when_start
            meter_value += int(entry.entry_value)
            entry_out['meter_value'] = meter_value
            meter_entries_out.append(entry_out)
            if is_irms:
                entry_out['spot_rms_current'] = int(entry.spot_rms_current)
                meter_entries_out.append(entry_out)

        last_entry = meter_entries_out[-1]

        self.meters[node_uuid]['when_last_meter_entry'] = last_entry['when_start']
        self.meters[node_uuid]['last_meter_value'] = last_entry['meter_value']
        if is_irms:
            self.meters[node_uuid]['last_rms_current'] = last_entry['spot_rms_current']

        self.meter_man.proc_meter_update(node_uuid, meter_entries_out)

        self.logger.info("Got meter update from node " + node_uuid + ".  Last entry was at " +
                         self.uts_to_str(last_entry['when_start']) + ' value: ' + str(last_entry['meter_value']) + 'Wh')


    def proc_meter_rebase(self, msg_obj):
        node_id = msg_obj['HEADER_1'].node_id
        node_uuid = self.get_node_uuid(msg_obj['network_id'], node_id)
        self.ensure_node_exists(node_uuid, node_id, msg_obj['gateway_uuid'])

        self.meters[node_uuid]['last_entry_timestamp'] = msg_obj['HEADER_1'].entry_timestamp
        self.meters[node_uuid]['last_meter_value'] = msg_obj['HEADER_1'].meter_value

        self.meter_man.proc_meter_rebase(node_uuid, msg_obj['HEADER_1'].entry_timestamp, msg_obj['HEADER_1'].meter_value)
        self.logger.info("Got meter rebase from node " + node_uuid + ".  Last entry was at " +
                         self.uts_to_str(msg_obj['HEADER_1'].entry_timestamp) + ' value: ' + msg_obj['HEADER_1'].meter_value + 'Wh')


    def proc_gateway_snapshot(self, msg_obj):
        # gateway object self-updates
        gateway_uuid = self.get_node_uuid(msg_obj['network_id'], msg_obj['gateway_id'])
        rec = msg_obj['HEADER_1']
        self.meter_man.proc_gateway_snapshot(gateway_uuid, msg_obj['when_received'], msg_obj['network_id'], msg_obj['gateway_id'], rec.when_booted,
                                             rec.free_ram, rec.gateway_time, rec.log_level,
                                             rec.encrypt_key, rec.tx_power)
        self.logger.info("Got gateway snapshot from gateway: " + gateway_uuid)


    def proc_node_snapshot(self, msg_obj):
        node_snapshots = {key: value for key, value in msg_obj.items() if key.startswith(gmsg.A_DETAIL)}
        if len(node_snapshots) == 0:
            self.logger.info("Got 0 node snapshots")
        else:
            for key, ns in node_snapshots.items():
                node_uuid = self.get_node_uuid(msg_obj['network_id'], ns.node_id)
                self.ensure_node_exists(node_uuid, ns.node_id, msg_obj['gateway_uuid'])
                self.meters[node_uuid] = {'network_id': msg_obj['network_id'], 'node_id': ns.node_id, 'gateway_id': msg_obj['gateway_id'], 'gateway_uuid': msg_obj['gateway_uuid'],
                                          'batt_voltage': ns.batt_voltage, 'up_time': ns.up_time, 'sleep_time': ns.sleep_time,
                                          'free_ram': ns.free_ram, 'when_last_seen': ns.when_last_seen,
                                          'last_clock_drift': ns.last_clock_drift, 'meter_interval': ns.meter_interval,
                                          'meter_impulses_per_kwh': ns.meter_impulses_per_kwh,
                                          'last_meter_entry_finish': ns.last_meter_entry_finish, 'last_meter_value': ns.last_meter_value,
                                          'last_rms_current': ns.last_rms_current, 'puck_led_rate': ns.puck_led_rate,
                                          'puck_led_time': ns.puck_led_time, 'last_rssi': ns.last_rssi_at_gateway}

                self.meter_man.proc_node_snapshot(node_uuid, msg_obj['when_received'], msg_obj['network_id'], ns.node_id, msg_obj['gateway_id'], ns.batt_voltage,
                                                    ns.up_time, ns.sleep_time, ns.free_ram, ns.when_last_seen, ns.last_clock_drift,
                                                    ns.meter_interval, ns.meter_impulses_per_kwh, ns.last_meter_entry_finish, ns.last_meter_value, ns.last_rms_current,
                                                    ns.puck_led_rate, ns.puck_led_time, ns.last_rssi_at_gateway)
                self.logger.info("Got node snapshot from node: " + node_uuid)


    def proc_node_dark(self, msg_obj):
        rec = msg_obj['HEADER_1']
        node_uuid = self.get_node_uuid(msg_obj['network_id'], rec.node_id)
        self.ensure_node_exists(node_uuid, rec.node_id, msg_obj['gateway_uuid'])
        self.meter_man.proc_node_dark(node_uuid, msg_obj['when_received'], rec.last_seen)
        self.logger.info("Got node dark from node: " + node_uuid + ".  Last seen at: " + self.uts_to_str(rec.last_seen))


    def proc_gp_msg(self, msg_obj):
        rec = msg_obj['HEADER_1']
        node_uuid = self.get_node_uuid(msg_obj['network_id'], rec.node_id)
        self.ensure_node_exists(node_uuid, rec.node_id, msg_obj['gateway_uuid'])
        self.meter_man.proc_gp_msg(node_uuid, msg_obj['when_received'], rec.message)
        self.logger.info("Got general-purpose message from node: " + node_uuid + " - " + rec.message)


    def proc_device_messages(self):
        # TODO: catch and handle garbled messages, e.g. 'MUP_;2,MUDEBUG:'
        # processing per gateway/network
        for key, gateway in self.gateways.items():
            # queued messages received from gateway, read according to last_rx_msg_obj sequential key
            new_msgs = {key: value for key, value in gateway['gw_obj'].serial_rx_msg_objects.items() if key > gateway['last_rx_msg_obj']}
            for key, new_msg in new_msgs.items():
                try:
                    if new_msg['message_type'] == gmsg.SMSG_MTRUPDATE_NO_IRMS_DEFN['smsg_type']:
                        self.proc_meter_update(new_msg, False)
                    elif new_msg['message_type'] == gmsg.SMSG_MTRUPDATE_WITH_IRMS_DEFN['smsg_type']:
                        self.proc_meter_update(new_msg, True)
                    elif new_msg['message_type'] == gmsg.SMSG_MTRREBASE_DEFN['smsg_type']:
                        self.proc_meter_rebase(new_msg)
                    elif new_msg['message_type'] == gmsg.SMSG_GWSNAP_DEFN['smsg_type']:
                        self.proc_gateway_snapshot(new_msg)
                    elif new_msg['message_type'] == gmsg.SMSG_NODESNAP_DEFN['smsg_type']:
                        self.proc_node_snapshot(new_msg)
                    elif new_msg['message_type'] == gmsg.SMSG_NODEDARK_DEFN['smsg_type']:
                        self.proc_node_dark(new_msg)
                    elif new_msg['message_type'] == gmsg.SMSG_GPMSG_DEFN['smsg_type']:
                        self.proc_gp_msg(new_msg)
                    else:
                        self.logger.warn("Got unknown message object: " + print(new_msg))
                except:
                    self.logger.error("Failed to process message object: " + print(new_msg))

                gateway['last_rx_msg_obj'] = key

            if gateway['last_clock_sync_time'] < arrow.utcnow().shift(seconds=-GATEWAY_TIME_SYNC_INTERVAL_SECS).timestamp:
                gateway['gw_obj'].set_gateway_time()
                gateway['last_clock_sync_time'] = arrow.utcnow().timestamp

            # updates from gateway/nodes
            if gateway['last_snap_time'] < arrow.utcnow().shift(seconds=-NODE_UPDATE_INTERVAL_SECS).timestamp:
                gateway['gw_obj'].get_gateway_snapshot()
                gateway['gw_obj'].get_node_snapshot()
                gateway['last_snap_time'] = arrow.utcnow().timestamp

            for node_uuid, sim_meter in gateway['sim_meters'].items():
                message_interval = int(sim_meter['max_msg_entries']) * int(sim_meter['interval'])
                if int(sim_meter['current_msg_start']) < arrow.utcnow().shift(seconds=-message_interval).timestamp:
                    if sim_meter['current_msg_start'] == 0:
                        sim_meter['current_msg_start'] = arrow.utcnow().shift(seconds=-message_interval).timestamp
                    sim_msg = 'G>S:MUP_;{},MUP_,{},{}'.format(sim_meter['node_id'], sim_meter['current_msg_start'], sim_meter['value'])
                    for i in range(1, int(sim_meter['max_msg_entries'])):
                        entry_value = randint(int(sim_meter['read_min']), int(sim_meter['read_max']) + 1)
                        sim_msg += ";{},{}".format(sim_meter['interval'], entry_value)
                        sim_meter['value'] += entry_value

                    sim_msg_obj = gmsg.get_message_obj(sim_msg,
                        gateway['gw_obj'].uuid, gateway['gw_obj'].gateway_id, gateway['gw_obj'].network_id)
                    sim_meter['current_msg_start'] = arrow.utcnow().timestamp
                    self.logger.debug("Generated simulated meter update: " + sim_msg)
                    self.proc_meter_update(sim_msg_obj, False)


    def add_meter_sim(self, network_id='0.0.1.1', gateway_id=1, node_id=100, interval=15, start_val=0,
                      read_min=0, read_max=20, max_msg_entries=4):

        gateway_uuid = self.get_node_uuid(network_id, gateway_id)
        node_uuid = self.get_node_uuid(network_id, node_id)

        if node_uuid not in self.gateways[gateway_uuid]['sim_meters']:
            self.gateways[gateway_uuid]['sim_meters'][node_uuid] = {}

        sim_meter = self.gateways[gateway_uuid]['sim_meters'][node_uuid]
        sim_meter['node_id'] = int(node_id)
        sim_meter['network_id'] = network_id
        sim_meter['interval'] = int(interval)
        sim_meter['value'] = int(start_val)
        sim_meter['read_min'] = int(read_min)
        sim_meter['read_max'] = int(read_max)
        sim_meter['max_msg_entries'] = int(max_msg_entries)
        sim_meter['current_msg_start'] = 0


    def del_meter_sim(self, gateway_uuid, node_uuid):
        if node_uuid in self.gateways[gateway_uuid]['sim_meters']:
            del self.gateways[gateway_uuid]['sim_meters'][node_uuid]

    def set_node_gw_inst_tmp_rate(self, node_uuid, tmp_ginr_poll_rate, tmp_ginr_poll_time):
        gateway_id = self.meters[node_uuid]['gateway_uuid']
        self.gateways[gateway_id]['gw_obj'].set_node_gw_inst_tmp_rate(self.meters[node_uuid]['node_id'], tmp_ginr_poll_rate, tmp_ginr_poll_time)


    def set_node_meter_value(self, node_uuid, new_meter_value):
        gateway_id = self.meters[node_uuid]['gateway_uuid']
        self.gateways[gateway_id]['gw_obj'].set_node_meter_value(self.meters[node_uuid]['node_id'], new_meter_value)


    def set_node_meter_interval(self, node_uuid, new_meter_interval):
        gateway_id = self.meters[node_uuid]['gateway_uuid']
        self.gateways[gateway_id]['gw_obj'].set_node_meter_interval(self.meters[node_uuid]['node_id'], new_meter_interval)


    def set_node_puck_led(self, node_uuid, new_puck_led_rate, new_puck_led_time):
        gateway_id = self.meters[node_uuid]['gateway_uuid']
        self.gateways[gateway_id]['gw_obj'].set_node_puck_led(self.meters[node_uuid]['node_id'], new_puck_led_rate, new_puck_led_time)


'''

================================================================================================================================================================
meter_man.py
=====================
out of band supervisor
notifications
api implementation, ext event messaging etc

not device and data handling; communication with these is async


================================================================================================================================================================

'''

from time import sleep

from meterman import app_base as base
from meterman import meter_device_manager as mdev_mgr
from meterman import meter_man_api
from uptime import boottime

from meterman import meter_data_manager as mdata_mgr
from meterman import meter_db as db


class MeterMan:

    def __init__(self):
        base.do_app_init()
        self.logger = base.get_logger(logger_name='meterman')
        self.logger.info('Running as user: ' + base.get_user())

        self.device_mgr = mdev_mgr.MeterDeviceManager(self)
        self.data_mgr = mdata_mgr.MeterDataManager()
        self.when_server_booted = boottime()
        self.simulate_meter = True

        rest_api_config = base.config['RestApi']
        if rest_api_config is not None and rest_api_config.getboolean('run_rest_api'):
            self.api_ctrl = meter_man_api.ApiCtrl(self, rest_api_config.getint('flask_port'), rest_api_config['user'],
                                                  rest_api_config['password'], rest_api_config['access_lan_only'])
            self.api_ctrl.run()


    def proc_meter_update(self, node_uuid, meter_entries):
        self.data_mgr.proc_meter_update(node_uuid, meter_entries)


    def proc_meter_rebase(self, node_uuid, entry_timestamp, meter_value):
        self.data_mgr.proc_meter_rebase(node_uuid, entry_timestamp, meter_value)


    def proc_gateway_snapshot(self, gateway_uuid, when_received, network_id, gateway_id, when_booted, free_ram, gw_time, log_level, encrypt_key,
                              tx_power):
        self.data_mgr.proc_gateway_snapshot(gateway_uuid, when_received, network_id, gateway_id, when_booted, free_ram, gw_time, log_level,
                              tx_power)     # omits encryption key


    def proc_node_snapshot(self, node_uuid, when_received, network_id, node_id, gateway_id, batt_voltage_mv, up_time, sleep_time, free_ram, last_seen,
                           last_clock_drift, meter_interval, meter_impulses_per_kwh, last_meter_read_at, last_meter_value, last_rms_current, puck_led_rate, puck_led_time,
                           last_rssi_at_gateway):
        self.data_mgr.proc_node_snapshot(node_uuid, when_received, network_id, node_id, gateway_id, batt_voltage_mv, up_time, sleep_time, free_ram, last_seen,
                                        last_clock_drift, meter_interval, meter_impulses_per_kwh, last_meter_read_at, last_meter_value, last_rms_current, puck_led_rate, puck_led_time,
                                         last_rssi_at_gateway)


    def proc_node_dark(self, node_uuid, when_received, last_seen):
        self.data_mgr.proc_node_event(node_uuid, when_received, db.NodeEventType.DARK.value, 'last seen at: ' + str(last_seen))


    def proc_gp_msg(self, node_uuid, when_received, message):
        if message.startswith('BOOT'):
            self.data_mgr.proc_node_event(node_uuid, when_received, db.NodeEventType.BOOT.value, message)


    def do_device_proc(self):
        # device_mgr = mdev_mgr.MeterDeviceManager(self)
        self.device_mgr.proc_device_messages()


    def register_node(self, node_uuid):
        pass


def main():
    meter_man = MeterMan()
    sleep(2)    # wait for meterman startup

    while True:
        meter_man.do_device_proc()
        sleep(0.5)

if __name__ == '__main__':
    main()


'''

================================================================================================================================================================
meter_data_manager.py
=====================

TODO: rename to reflect broader than meter data

================================================================================================================================================================

'''


from meterman import meter_db as db, app_base as base


class MeterDataManager:

    def __init__(self, db_file=base.db_file):
        self.logger = base.get_logger(logger_name='data_mgr')
        self.db_mgr = db.DBManager(db_file)

        self.do_ev_file = False
        ev_file_config = None

        if base.config is not None:
            ev_file_config = base.config['EventFile']

        if ev_file_config is not None and ev_file_config.getboolean('write_event_file'):
            self.ev_logger = base.get_logger('ev_logger', base.home_path + ev_file_config['event_file'], True)
            self.do_ev_file = True
            self.ev_log_meter_only = ev_file_config.getboolean('meter_only')


    def close_db(self):
        self.db_mgr.conn_close()
        self.db_mgr = None


    def dictlist_from_rows(self, rows):
        if rows is None:
            return

        dictlist = []
        for row in rows:
            dictlist.append(dict(zip(row.keys(), row)))
        return dictlist


    def proc_gateway_snapshot(self, gateway_uuid, when_received, network_id, gateway_id, when_booted, free_ram, gateway_time, log_level, tx_power):
        self.db_mgr.write_gateway_snapshot(gateway_uuid, int(when_received), network_id, int(gateway_id), int(when_booted), int(free_ram), int(gateway_time), log_level,
                          int(tx_power), db.RecStatus.NORMAL.value)
        if self.do_ev_file and (not self.ev_log_meter_only):
            self.ev_logger.info("{},{},{},{},{},{},{},{},{},{}".format(
                                    'GWSNAP', gateway_uuid, when_received, network_id, gateway_id, when_booted, free_ram, gateway_time, log_level,
                                    tx_power))


    def get_gw_snapshots(self, gateway_uuid=None, time_from=None, time_to=None, rec_status=None, limit_count=None):
        return self.dictlist_from_rows(self.db_mgr.get_gateway_snapshots(gateway_uuid, time_from, time_to, rec_status, limit_count))


    def get_node_snapshots(self, node_uuid=None, network_id=None, time_from=None, time_to=None, rec_status=None, limit_count=None):
        return self.dictlist_from_rows(self.db_mgr.get_node_snapshots(node_uuid, network_id, time_from, time_to, rec_status, limit_count))


    def get_node_events(self, node_uuid=None, time_from=None, time_to=None, event_type=None, limit_count=None):
        return self.dictlist_from_rows(self.db_mgr.get_node_events(node_uuid, time_from, time_to, event_type, limit_count))


    def proc_node_snapshot(self, node_uuid, when_received, network_id, node_id, gateway_id, batt_voltage_mv, up_time, sleep_time, free_ram, when_last_seen,
                           last_clock_drift, meter_interval, meter_impulses_per_kwh, last_meter_entry_finish, last_meter_value, last_rms_current, puck_led_rate, puck_led_time,
                           last_rssi_at_gateway):
        self.db_mgr.write_node_snapshot(node_uuid, int(when_received), network_id, int(node_id), int(gateway_id), int(batt_voltage_mv), int(up_time),
                                        int(sleep_time), int(free_ram), int(when_last_seen), int(last_clock_drift), int(meter_interval), int(meter_impulses_per_kwh), int(last_meter_entry_finish),
                                        int(last_meter_value), float(last_rms_current), int(puck_led_rate), int(puck_led_time), int(last_rssi_at_gateway), db.RecStatus.NORMAL.value)
        if self.do_ev_file and (not self.ev_log_meter_only):
            self.ev_logger.info("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(
                                    'NODESNAP', node_uuid, when_received, network_id, node_id, gateway_id, batt_voltage_mv, up_time, sleep_time, free_ram, when_last_seen,
                                    last_clock_drift, meter_interval, meter_impulses_per_kwh, last_meter_entry_finish, last_meter_value, last_rms_current, puck_led_rate, puck_led_time,
                                    last_rssi_at_gateway))


    def get_meter_entries(self, node_uuid=None, entry_type=None, rec_status=None, time_from=None, time_to=None, query_type=None, limit_count=1000):
        return self.dictlist_from_rows(self.db_mgr.get_node_meter_entries(node_uuid, entry_type, rec_status, time_from, time_to, limit_count))


    def get_meter_consumption(self, node_uuid, time_from=None, time_to=None):
        # Get min/max rebase entries within interval, treat consumption BETWEEN these as authoritative and count it.
        # Then add any actual observed consumption (i.e. watt-hours from MeterNode 'reads') prior to the first and last rebase

        first_mup_entry = None
        mup_entry_before_first_rebase = None
        first_rebase_entry = None
        last_rebase_entry = None
        last_mup_entry = None

        first_mup_entry = self.db_mgr.get_first_mup(node_uuid, time_from, time_to)
        last_mup_entry = self.db_mgr.get_last_mup(node_uuid, time_from, time_to)

        # check if <=1 entries
        if first_mup_entry is None or last_mup_entry is None:
            return 0

        # get first rebase in period
        first_rebase_entry = self.db_mgr.get_first_rebase(node_uuid, time_from, time_to)

        # if there is a first rebase, get immediate entry prior, and check for subsequent rebase
        if first_rebase_entry is not None:
            mup_entry_before_first_rebase = self.db_mgr.get_last_mup(node_uuid, time_from, first_rebase_entry['when_start'] - 1)
            last_rebase_entry = self.db_mgr.get_last_rebase(node_uuid, time_from, time_to)
            if last_rebase_entry['when_start'] == first_rebase_entry['when_start']:
                last_rebase_entry = None

        total_consumption = 0
        # simple case, no rebases
        if first_rebase_entry is None:
            total_consumption = last_mup_entry['meter_value'] - first_mup_entry['meter_value']
            return total_consumption

        # entries prior to first rebase, use observed reads for period before rebase
        elif mup_entry_before_first_rebase is not None and first_mup_entry['when_start'] < first_rebase_entry['when_start']:
            total_consumption = mup_entry_before_first_rebase['meter_value'] - first_mup_entry['meter_value']

        # with >1 rebase
        if last_rebase_entry is not None:
            total_consumption += last_rebase_entry['meter_value'] - first_rebase_entry['meter_value']
            # entries after last rebase
            if last_mup_entry['when_start'] >= last_rebase_entry['when_start']:
                total_consumption += last_mup_entry['meter_value'] - last_rebase_entry['meter_value']
        # only 1 rebase prior to last mup
        elif last_mup_entry['when_start'] >= first_rebase_entry['when_start']:
            total_consumption += last_mup_entry['meter_value'] - first_rebase_entry['meter_value']
        # only 1 rebase after last mup
        elif last_mup_entry['when_start'] <= first_rebase_entry['when_start']:
            total_consumption += first_rebase_entry['meter_value'] - last_mup_entry['meter_value']

        self.logger.debug('Calculated consumption as {} Wh with:'.format(total_consumption))
        self.logger.debug('First MUP Entry: {}'.format(first_mup_entry['meter_value'] if first_mup_entry is not None else None))
        self.logger.debug('MUP Entry before first rebase: {}'.format(mup_entry_before_first_rebase['meter_value'] if mup_entry_before_first_rebase is not None else None))
        self.logger.debug('First Rebase Entry: {}'.format(first_rebase_entry['meter_value'] if first_rebase_entry is not None else None))
        self.logger.debug('Last Rebase Entry: {}'.format(last_rebase_entry['meter_value'] if last_rebase_entry is not None else None))
        self.logger.debug('Last MUP Entry: {}'.format(last_mup_entry['meter_value'] if last_mup_entry is not None else None))

        return total_consumption


    def proc_meter_update(self, node_uuid, meter_entries):
        for entry in meter_entries:
            timestamp_nonce = base.get_nonce()

            self.db_mgr.write_meter_entry(node_uuid, int(entry['when_start']), timestamp_nonce, int(entry['when_start']), db.EntryType.METER_UPDATE.value,
                                          int(entry['entry_value']), int(entry['entry_interval_length']), int(entry['meter_value']), db.RecStatus.NORMAL.value)
            if self.do_ev_file:
                self.ev_logger.info("{},{},{},{},{},{},{},{},{},{}".format('MTRUPDATE', node_uuid, int(entry['when_start']), timestamp_nonce, int(entry['when_start']), db.EntryType.METER_UPDATE.value,
                                          int(entry['entry_value']), int(entry['entry_interval_length']), int(entry['meter_value']), db.RecStatus.NORMAL.value))


    def proc_meter_rebase(self, node_uuid, entry_timestamp, meter_value):
        #TODO: handle more intelligently, implement definitive master - consider that meter node cannot be reached in realtime
        #meter wins except for reboot, rollover? metervalue as utterly notional except to track accuracy vs smart meter? What really matters is use in time period...
        timestamp_nonce = base.get_nonce()
        self.db_mgr.write_meter_entry(node_uuid, int(entry_timestamp), timestamp_nonce, int(entry_timestamp), db.EntryType.METER_REBASE.value, 0, 0, int(meter_value), db.RecStatus.NORMAL.value)
        if self.do_ev_file:
            self.ev_logger.info("{},{},{},{},{},{},{}".format('MTRREBASE', int(entry_timestamp), timestamp_nonce, int(entry_timestamp), db.EntryType.METER_REBASE.value, int(meter_value), db.RecStatus.NORMAL.value))


    def proc_node_event(self, node_uuid, timestamp, event_type, details=None):
        self.db_mgr.write_node_event(node_uuid, timestamp, event_type, details)


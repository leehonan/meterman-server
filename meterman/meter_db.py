'''

================================================================================================================================================================
meter_db.py
=====================
TODO: rename

================================================================================================================================================================

'''

import sqlite3
from enum import Enum

from meterman import app_base as base


# Database Record Statuses
class RecStatus(Enum):
    NORMAL = 'NORM'
    HIDDEN = 'HDN'
    DELETED = 'DEL'

# Meter Entry Types
class EntryType(Enum):
    METER_UPDATE = 'MUP'
    METER_REBASE = 'MREB'
    METER_UPDATE_SYNTH = 'MUPS'
    METER_REBASE_SYNTH = 'MREBS'

# Node Event Types
class NodeEventType(Enum):
    BOOT = 'BOOT'
    DARK = 'DARK'
    LOW_BATT = 'LBATT'

# noinspection SqlDialectInspection
class DBManager:
    """
    TBD...

    """


    def do_vacuum(self):
        self.connection.isolation_level = None
        self.connection.execute("VACUUM")  # TODO: confirm won't pause startup for too long
        self.connection.isolation_level = ""


    def __init__(self, database_file_uri):

        try:
            self.logger = base.get_logger(logger_name='db_mgr')
            self.db_uri = database_file_uri
            self.connection = sqlite3.connect(database_file_uri, check_same_thread=False)       # TODO: ensure access thread-safe
            self.connection.row_factory = sqlite3.Row

            cursor = self.connection.cursor()

            cursor.execute('SELECT sqlite_version()')
            self.db_version = cursor.fetchone()
            self.logger.info('Connected to sqlite DB.  Version is: {0}'.format(self.db_version))

            cursor.execute('CREATE TABLE IF NOT EXISTS meter_entry ('
                            'node_uuid data_type TEXT NOT NULL, '
                            'when_start_raw data_type INTEGER NOT NULL, '
                            'when_start_raw_nonce data_type TEXT NOT NULL, '
                            'when_start data_type INTEGER NOT NULL, '
                            'duration data_type INTEGER NOT NULL, '
                            'entry_type data_type TEXT NOT NULL, '
                            'entry_value data_type INTEGER NOT NULL, '
                            'meter_value data_type INTEGER NOT NULL, '
                            'rec_status data_type TEXT NOT NULL, '
                            'PRIMARY KEY (node_uuid, when_start_raw, when_start_raw_nonce)) WITHOUT ROWID')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_meter_entry_node_uuid ON meter_entry (node_uuid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_meter_entry_when_start ON meter_entry (when_start)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_meter_entry_entry_type ON meter_entry (entry_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_meter_entry_rec_status ON meter_entry (rec_status)')

            cursor.execute('CREATE TABLE IF NOT EXISTS gateway_snapshot ('
                           'gateway_uuid data_type TEXT NOT NULL, '
                           'when_received data_type INTEGER NOT NULL, '
                           'network_id data_type TEXT NOT NULL, '
                           'gateway_id data_type INTEGER NOT NULL, '
                           'when_booted data_type INTEGER NOT NULL, '
                           'free_ram data_type INTEGER NOT NULL, '
                           'gateway_time data_type INTEGER NOT NULL, '
                           'log_level data_type TEXT NOT NULL, '
                           'tx_power data_type INTEGER NOT NULL, '
                           'rec_status data_type TEXT NOT NULL, '
                           'PRIMARY KEY (gateway_uuid, when_received)) WITHOUT ROWID')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_gateway_snapshot_uuid ON gateway_snapshot (gateway_uuid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_gateway_snapshot_when_received ON gateway_snapshot (when_received)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_gateway_snapshot_rec_status ON gateway_snapshot (rec_status)')

            cursor.execute('CREATE TABLE IF NOT EXISTS node_snapshot ('
                           'node_uuid data_type TEXT NOT NULL, '
                           'when_received data_type INTEGER NOT NULL, '
                           'network_id data_type TEXT NOT NULL, '
                           'node_id data_type INTEGER NOT NULL, '
                           'gateway_id data_type INTEGER NOT NULL, '
                           'batt_voltage_mv data_type INTEGER NOT NULL, '
                           'up_time data_type INTEGER NOT NULL, '
                           'sleep_time data_type INTEGER NOT NULL, '
                           'free_ram data_type INTEGER NOT NULL, '
                           'when_last_seen data_type INTEGER NOT NULL, '
                           'last_clock_drift data_type INTEGER NOT NULL, '
                           'meter_interval data_type INTEGER NOT NULL, '
                           'meter_impulses_per_kwh data_type INTEGER NOT NULL, '
                           'last_meter_entry_finish data_type INTEGER NOT NULL, '
                           'last_meter_value data_type INTEGER NOT NULL, '
                           'last_rms_current data_type REAL NOT NULL, '
                           'puck_led_rate data_type INTEGER NOT NULL, '
                           'puck_led_time data_type INTEGER NOT NULL, '
                           'last_rssi_at_gateway data_type INTEGER NOT NULL, '
                           'rec_status data_type TEXT NOT NULL, '
                           'PRIMARY KEY (node_uuid, when_received)) WITHOUT ROWID')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_node_snapshot_uuid ON node_snapshot (node_uuid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_node_snapshot_when_received ON node_snapshot (when_received)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_node_snapshot_network_id ON node_snapshot (network_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_node_snapshot_rec_status ON node_snapshot (rec_status)')

            cursor.execute('CREATE TABLE IF NOT EXISTS node_event ('
                            'event_id data_type INTEGER PRIMARY KEY, '
                            'node_uuid data_type TEXT NOT NULL, '
                            'timestamp data_type INT NOT NULL, '
                            'event_type  data_type TEXT NOT NULL, '
                            'details data_type TEXT NOT NULL)')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_node_event_node_uuid ON node_event (node_uuid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_node_event_timestamp ON node_event (timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_node_event_event_type ON node_event (event_type)')

            cursor.execute('CREATE TABLE IF NOT EXISTS sys_param ('
                           'name data_type TEXT NOT NULL, '
                           'value data_type TEXT NOT NULL, '
                           'PRIMARY KEY (name)) WITHOUT ROWID')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sys_param_name ON sys_param (name)')

            cursor.execute('CREATE TABLE IF NOT EXISTS user ('
                           'username data_type TEXT NOT NULL, '
                           'password data_type TEXT NOT NULL, '
                           'permissions data_type TEXT NOT NULL, '            
                           'PRIMARY KEY (username)) WITHOUT ROWID')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_username ON user (username)')

            self.connection.commit()
            cursor.close()

            self.do_vacuum()

        except sqlite3.Error as err:
            self.logger.info('sqlite3 Error: {0}'.format(err))


    def conn_open(self):
        self.connection = sqlite3.connect(self.db_uri)
        self.connection.row_factory = sqlite3.Row

    def conn_close(self):
        self.connection.commit()  # redundant, just in case
        self.connection.close()


    def __exit__(self, exc_type, exc_value, traceback):
        self.conn_close()   # redundant, just in case


    def write_meter_entry(self, node_uuid, when_start_raw, when_start_raw_nonce, when_start, entry_type, entry_value, duration, meter_value, rec_status):
        try:
            cursor = self.connection.cursor()
            cmd = 'INSERT INTO meter_entry (node_uuid, when_start_raw, when_start_raw_nonce, when_start, entry_type, entry_value, duration, meter_value, rec_status)' \
                  ' VALUES ("{0}", {1}, "{2}", {3}, "{4}", {5}, {6}, {7}, "{8}")' \
                .format(node_uuid, when_start_raw, when_start_raw_nonce, when_start, entry_type, entry_value, duration, meter_value, rec_status)
            cursor.execute(cmd)
            self.logger.debug('Inserted meter_entry record for PRIMARY KEY [{0},{1},{2}]'.format(node_uuid, when_start_raw, when_start_raw_nonce))
            self.connection.commit()
            cursor.close()

        except sqlite3.IntegrityError:
            self.logger.warn('ERROR: ID already exists in PRIMARY KEY [{0},{1},{2}]'.format(node_uuid, when_start_raw, when_start_raw_nonce))

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def update_meter_entry(self, node_uuid, when_start_raw, when_start_raw_nonce, when_start, entry_type, entry_value, duration, meter_value, rec_status):
        try:

            if node_uuid is None or when_start_raw is None or when_start_raw_nonce is None:
                raise ValueError('Primary key not given.  Got of node_uuid={0), when_start_raw={1}, when_start_raw_nonce={2}.'.format(node_uuid, when_start_raw, when_start_raw_nonce))

            if when_start is None or entry_type is None or meter_value is None or rec_status is None:
                raise ValueError('No update columns given.')

            # Build SQL update command...
            cmd = 'UPDATE meter_entry SET '

            if when_start is not None:
                cmd += 'when_start = {},'.format(when_start)
            if entry_type is not None:
                cmd += 'entry_type = "{}",'.format(entry_type)
            if entry_value is not None:
                cmd += 'entry_value = {},'.format(entry_value)
            if duration is not None:
                cmd += 'duration = {},'.format(duration)
            if meter_value is not None:
                cmd += 'meter_value = {},'.format(meter_value)
            if rec_status is not None:
                cmd += 'rec_status = "{}",'.format(rec_status)

            cmd = cmd[:-1] + ' '      # replace trailing comma with space

            cmd += 'WHERE node_uuid = "{0}", when_start_raw = {1}, when_start_raw_nonce = "{2}"'.format(
                node_uuid, when_start_raw, when_start_raw_nonce
                )

            cursor = self.connection.cursor()
            cursor.execute(cmd)
            self.logger.debug('Updated meter_entry record for PRIMARY KEY [{0},{1},{2}]'.format(node_uuid, when_start_raw, when_start_raw_nonce))
            self.connection.commit()
            cursor.close()

        except ValueError as err:
            self.logger.warn('Value Error: {0}'.format(err))

        except sqlite3.IntegrityError as err:
            self.logger.warn('sqlite3 IntegrityError: {0}'.format(err))

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def get_node_meter_entries_count(self, node_uuid=None, entry_type=None, rec_status=None):
        cmd = 'SELECT COUNT(*) FROM meter_entry'

        if any(param is not None for param in [node_uuid, entry_type, rec_status]):
            cmd += ' WHERE '
        if node_uuid is not None:
            cmd += 'node_uuid = "{}" AND '.format(node_uuid)
        if entry_type is not None:
            cmd += 'entry_type = "{}" AND '.format(entry_type)
        if rec_status is not None:
            cmd += 'rec_status = "{}" AND '.format(rec_status)

        if cmd.endswith('AND '):
            cmd = cmd[:-4] + ' '  # replace trailing "AND " with space
        cursor = self.connection.cursor()
        cursor.execute(cmd)
        count = cursor.fetchall()
        cursor.close()
        return count[0][0]


    def get_node_meter_entries(self, node_uuid=None, entry_type=None, rec_status=None, time_from=None, time_to=None,
                               limit_count=1000):
        try:
            # Build SQL update command...
            cmd = 'SELECT * FROM meter_entry'

            if any(param is not None for param in [node_uuid, entry_type, rec_status]):
                cmd += ' WHERE '
            if node_uuid is not None:
                cmd += 'node_uuid = "{}" AND '.format(node_uuid)
            if entry_type is not None:
                cmd += 'entry_type = "{}" AND '.format(entry_type)
            if rec_status is not None:
                cmd += 'rec_status = "{}" AND '.format(rec_status)
            if time_from is not None:
                cmd += 'when_start >= {} AND '.format(time_from)
            if time_to is not None:
                cmd += 'when_start <= {} AND '.format(time_to)

            if cmd.endswith('AND '):
                cmd = cmd[:-4] + ' '  # replace trailing "AND " with space

            if limit_count is not None:
                cmd += 'ORDER BY when_start DESC LIMIT {0}'.format(limit_count)

            cursor = self.connection.cursor()
            cursor.execute(cmd)
            rows = cursor.fetchall()
            cursor.close()
            return rows

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def get_entry(self, node_uuid, is_rebase=False, is_first=True, time_from=None, time_to=None):
        try:
            # Build SQL update command...
            min_max = 'min' if is_first else 'max'
            entry_types = [EntryType.METER_REBASE, EntryType.METER_REBASE_SYNTH] if is_rebase \
                            else [EntryType.METER_UPDATE, EntryType.METER_UPDATE_SYNTH]
            cmd = 'SELECT * FROM meter_entry ' \
                  'WHERE when_start = ' \
                  '(SELECT {}(when_start) FROM meter_entry ' \
                  'WHERE node_uuid="{}" AND entry_type IN ("{}","{}") AND ' \
                    'rec_status="{}" AND'.format(min_max, node_uuid, entry_types[0].value, entry_types[1].value, RecStatus.NORMAL.value)

            if time_from is not None:
                cmd += ' when_start >= {} AND'.format(time_from)
            if time_to is not None:
                cmd += ' when_start <= {} AND'.format(time_to)
            if cmd.endswith(' AND'):
                cmd = cmd[:-4]  # replace trailing " AND"

            min_max = 'ASC' if is_first else 'DESC'
            cmd += ') ORDER BY when_start {} LIMIT 1'.format(min_max)

            cursor = self.connection.cursor()
            cursor.execute(cmd)
            rows = cursor.fetchone()
            cursor.close()
            return rows

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def get_first_mup(self, node_uuid, time_from=None, time_to=None):
        return self.get_entry(node_uuid, is_rebase=False, is_first=True, time_from=time_from, time_to=time_to)


    def get_last_mup(self, node_uuid, time_from, time_to):
        return self.get_entry(node_uuid, is_rebase=False, is_first=False, time_from=time_from, time_to=time_to)


    def get_first_rebase(self, node_uuid, time_from, time_to):
        return self.get_entry(node_uuid, is_rebase=True, is_first=True, time_from=time_from, time_to=time_to)


    def get_last_rebase(self, node_uuid, time_from, time_to):
        return self.get_entry(node_uuid, is_rebase=True, is_first=False, time_from=time_from, time_to=time_to)


    def delete_node_meter_entry(self, node_uuid, when_start_raw, when_start_raw_nonce):
        try:
            cursor = self.connection.cursor()
            cursor.execute('DELETE FROM meter_entry WHERE node_uuid = "{0}" AND when_start_raw = {1} AND when_start_raw_nonce = "{2}"'.format(
                node_uuid, when_start_raw, when_start_raw_nonce
            ))
            cursor.close()

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def write_gateway_snapshot(self, gateway_uuid, when_received, network_id, gateway_id, when_booted, free_ram,
                               gateway_time, log_level, tx_power, rec_status):
        try:
            cursor = self.connection.cursor()
            cmd = 'INSERT INTO gateway_snapshot (gateway_uuid, when_received, network_id, gateway_id, when_booted, free_ram, \
                        gateway_time, log_level, tx_power, rec_status)' \
                  ' VALUES ("{0}", {1}, "{2}", {3}, {4}, {5}, {6}, "{7}", {8}, "{9}")' \
                .format(gateway_uuid, when_received, network_id, gateway_id, when_booted, free_ram,
                        gateway_time, log_level, tx_power, rec_status)
            cursor.execute(cmd)
            self.logger.debug('Inserted gateway_snapshot record for PRIMARY KEY [{0},{1}]'.format(gateway_uuid, when_received))
            self.connection.commit()
            cursor.close()

        except sqlite3.IntegrityError:
            self.logger.warn('ERROR: ID already exists in PRIMARY KEY [{0},{1}]'.format(gateway_uuid, when_received))

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def get_gateway_snapshots(self, gateway_uuid=None, time_from=None, time_to=None, rec_status=None, limit_count=1):
        """
        Simple query function.  Use direct SQL otherwise.

        """

        try:
            # Build SQL update command...
            cmd = 'SELECT * FROM gateway_snapshot'

            if gateway_uuid is not None or rec_status is not None:
                cmd += ' WHERE '
            if gateway_uuid is not None:
                cmd += 'gateway_uuid = "{}" AND '.format(gateway_uuid)

            if rec_status is not None:
                cmd += 'rec_status = "{}" AND '.format(rec_status)

            if time_from is not None:
                cmd += 'when_received >= {} AND '.format(time_from)

            if time_to is not None:
                cmd += 'when_received <= {} AND '.format(time_to)

            if cmd.endswith('AND '):
                cmd = cmd[:-4] + ' '  # replace trailing "AND " with space

            if limit_count is not None:
                cmd += ' ORDER BY when_received DESC LIMIT {0}'.format(limit_count)

            cursor = self.connection.cursor()
            cursor.execute(cmd)
            rows = cursor.fetchall()
            cursor.close()
            return rows

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def write_node_snapshot(self, node_uuid, when_received, network_id, node_id, gateway_id, batt_voltage_mv, up_time, sleep_time, free_ram, when_last_seen, last_clock_drift,
                            meter_interval, meter_impulses_per_kwh, last_meter_entry_finish, last_meter_value, last_rms_current, puck_led_rate, puck_led_time, last_rssi_at_gateway, rec_status):
        try:
            cursor = self.connection.cursor()
            cmd = 'INSERT INTO node_snapshot (node_uuid, when_received, network_id, node_id, gateway_id, batt_voltage_mv, up_time, sleep_time, free_ram, when_last_seen, ' \
                                                'last_clock_drift, meter_interval, meter_impulses_per_kwh, last_meter_entry_finish, last_meter_value, last_rms_current, puck_led_rate,  puck_led_time, ' \
                                                'last_rssi_at_gateway, rec_status) ' \
                                                'VALUES ("{0}", {1}, "{2}", {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, "{19}")'.format(
                                                        node_uuid, when_received, network_id, node_id, gateway_id, batt_voltage_mv, up_time, sleep_time, free_ram,
                                                        when_last_seen, last_clock_drift, meter_interval, meter_impulses_per_kwh, last_meter_entry_finish, last_meter_value, last_rms_current, puck_led_rate,
                                                        puck_led_time, last_rssi_at_gateway, rec_status)
            cursor.execute(cmd)
            self.logger.debug('Inserted node_snapshot record for PRIMARY KEY [{0},{1}]'.format(node_uuid, when_received))
            self.connection.commit()
            cursor.close()

        except sqlite3.IntegrityError:
            self.logger.warn('ERROR: ID already exists in PRIMARY KEY [{0},{1}]'.format(node_uuid, when_received))

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def get_node_snapshots(self, node_uuid=None, network_id=None, time_from=None, time_to=None, rec_status=None, limit_count=1):

        try:
            # Build SQL update command...
            cmd = 'SELECT * FROM node_snapshot'

            if any(param is not None for param in [node_uuid, network_id, rec_status]):
                cmd += ' WHERE '
            if node_uuid is not None:
                cmd += 'node_uuid = "{}" AND '.format(node_uuid)
            if time_from is not None:
                cmd += 'when_received >= {} AND '.format(time_from)
            if time_to is not None:
                cmd += 'when_received <= {} AND '.format(time_to)
            if network_id is not None:
                cmd += 'network_id = {} AND '.format(network_id)
            if rec_status is not None:
                cmd += 'rec_status = "{}" AND '.format(rec_status)

            if cmd.endswith('AND '):
                cmd = cmd[:-4] + ' '  # replace trailing "AND " with space

            if limit_count is not None:
                cmd += ' ORDER BY when_received DESC LIMIT {0}'.format(limit_count)

            cursor = self.connection.cursor()
            cursor.execute(cmd)
            rows = cursor.fetchall()
            cursor.close()
            return rows

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def write_node_event(self, node_uuid, timestamp, event_type, details):
        try:
            cursor = self.connection.cursor()
            cmd = 'INSERT INTO node_event (node_uuid, timestamp, event_type, details)' \
                  ' VALUES ("{0}", {1}, "{2}", "{3}")' \
                .format(node_uuid, timestamp, event_type, details)
            cursor.execute(cmd)
            self.logger.debug('Inserted node_event record with node={0}, event type={1}'.format(node_uuid, event_type))
            self.connection.commit()
            cursor.close()

        except sqlite3.IntegrityError:
            self.logger.warn('ERROR: ID already exists in PRIMARY KEY ')

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def get_node_events(self, node_uuid=None, time_from=None, time_to=None, event_type=None, limit_count=1):

        try:
            # Build SQL update command...
            cmd = 'SELECT * FROM node_event'
            if any(param is not None for param in [node_uuid, time_from, time_to, event_type]):
                cmd += ' WHERE '
            if node_uuid is not None:
                cmd += 'node_uuid = "{}" AND '.format(node_uuid)
            if time_from is not None:
                cmd += 'timestamp >= {} AND '.format(time_from)
            if time_to is not None:
                cmd += 'timestamp <= {} AND '.format(time_to)
            if event_type is not None:
                cmd += 'event_type = "{}" AND '.format(event_type)

            if cmd.endswith('AND '):
                cmd = cmd[:-4] + ' '  # replace trailing "AND " with space

            if limit_count is not None:
                cmd += ' ORDER BY timestamp DESC LIMIT {0}'.format(limit_count)

            cursor = self.connection.cursor()
            cursor.execute(cmd)
            rows = cursor.fetchall()
            cursor.close()
            return rows

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def write_sys_param(self, name, value):
        try:
            cursor = self.connection.cursor()
            cmd = 'INSERT INTO sys_param (name, value)' \
                  ' VALUES ("{0}", {1})' \
                .format(name, value)
            cursor.execute(cmd)
            self.logger.debug('Inserted sys_param record with name={0}, value={1}'.format(name, value))
            self.connection.commit()
            cursor.close()

        except sqlite3.IntegrityError:
            self.logger.warn('ERROR: ID already exists in PRIMARY KEY [{0}]'.format(name))

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def get_sys_param(self, name):
        """
        Simple query function.  Use direct SQL otherwise.

        """

        try:
            # Build SQL update command...
            cmd = 'SELECT * FROM sys_param WHERE name = "{}"'.format(name)
            cursor = self.connection.cursor()
            cursor.execute(cmd)
            rows = cursor.fetchall()
            cursor.close()
            return rows

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def update_sys_param(self, name, value):
        try:

            if name is None or value is None:
                raise ValueError('Invalid sys_param update.  Got of name={0), value={1}.'.format(name, value))

            # Build SQL update command...
            cmd = 'UPDATE sys_param SET value = {0} WHERE name = "{1}"'.format(value, name)

            cursor = self.connection.cursor()
            cursor.execute(cmd)
            self.logger.debug('Updated sys_param record for PRIMARY KEY [{0}]'.format(name))
            self.connection.commit()
            cursor.close()

        except ValueError as err:
            self.logger.warn('Value Error: {0}'.format(err))

        except sqlite3.IntegrityError as err:
            self.logger.warn('sqlite3 IntegrityError: {0}'.format(err))

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def write_user(self, username, password, permissions):
        try:
            cursor = self.connection.cursor()
            cmd = 'INSERT INTO user (username, password, permissions)' \
                  ' VALUES ("{0}", {1}, {2})' \
                .format(username, password, permissions)
            cursor.execute(cmd)
            self.logger.debug('Inserted user record with username={0}'.format(username))
            self.connection.commit()
            cursor.close()

        except sqlite3.IntegrityError:
            self.logger.warn('ERROR: ID already exists in PRIMARY KEY [{0}]'.format(username))

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def get_user(self, username):
        """
        Simple query function.  Use direct SQL otherwise.

        """

        try:
            # Build SQL update command...
            cmd = 'SELECT * FROM user WHERE username = "{}"'.format(username)
            cursor = self.connection.cursor()
            cursor.execute(cmd)
            rows = cursor.fetchall()
            cursor.close()
            return rows

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))


    def update_user(self, username, password, permissions):
        try:

            if username is None or (password is None and permissions is None):
                raise ValueError('Invalid user update for username={0)'.format(username))

            # Build SQL update command...
            cmd = 'UPDATE user SET '

            if password is not None:
                cmd += 'password = {0}'.format(password) + ' AND '

            if permissions is not None:
                cmd += 'permissions = {0}'.format(permissions) + ' AND '

            if cmd.endswith('AND '):
                cmd = cmd[:-4] + ' '  # replace trailing "AND " with space

            cmd += 'WHERE username = "{0}"'.format(username)

            cursor = self.connection.cursor()
            cursor.execute(cmd)
            self.logger.debug('Updated user record for PRIMARY KEY [{0}]'.format(username))
            self.connection.commit()
            cursor.close()

        except ValueError as err:
            self.logger.warn('Value Error: {0}'.format(err))

        except sqlite3.IntegrityError as err:
            self.logger.warn('sqlite3 IntegrityError: {0}'.format(err))

        except sqlite3.Error as err:
            self.logger.warn('sqlite3 Error: {0}'.format(err))

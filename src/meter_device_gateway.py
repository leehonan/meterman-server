'''

================================================================================================================================================================
meter_device_gateway.py
=====================

Implements state management of and integration with a Meter Gateway, which is connected to the machine running this code (server)
using UART serial.  The reference design for the Meter Gateway is a 'PiGate' board that mounts onto a a Raspberry Pi, connecting through its GPIO pins,
and communicates with Meter Nodes (the 'Horus' board and its sensor(s)) using a RFM69 packet radio.

The Meter Gateway is essentially a conduit between the implementation here and the Meter Node(s) connected to it.  It maintains minimal state on these nodes
and passes all metering data through for ingest by the server.

While this implementation includes everything needed to represent and control the gateway, it doesn't include higher-order control and supervision of the
metering system (or sub-system if part of a broader solution).  Nor does it encapsulate the nodes connected to it - that is a largely stylistic decision in
favour of a flatter rather than nested structuring.  It does, however implement the messaging needed to control a particular node (which is called through
more abstracted in the implementation of the node class).

The implementation assumes (for now) that all meter nodes are metering the same thing, with the same unit of measure.  As the implementation is only being used
for metering electricity consumption, this is likely to weaken the attempts made at abstraction through the implementation.

================================================================================================================================================================

'''

# ==============================================================================================================================================================
#  IMPORTS
# ==============================================================================================================================================================

import threading
from enum import Enum
from time import sleep

import arrow
from meterman import gateway_messages as gmsg
import serial

from meterman import app_base as base

# ==============================================================================================================================================================
#  GLOBAL CONSTANTS
# ==============================================================================================================================================================

DEF_SERIAL_PORT = '/dev/ttyAMA0'
DEF_SERIAL_BAUD = 115200

A_UNKNOWN = 'UNKNOWN'

PURGE_RX_MSG_AGE_SECS = 600

# Device Statuses
class DeviceStatus(Enum):
    INIT = 0
    UP = 1
    DARK = 2


# ==============================================================================================================================================================
#  GLOBAL VARS
# ==============================================================================================================================================================



# ==============================================================================================================================================================
#  IMPLEMENTATION
# ==============================================================================================================================================================

class MeterDeviceGateway:

    def __init__(self, meter_device_manager, network_id, gateway_id, label='Gateway', serial_port=DEF_SERIAL_PORT, serial_baud=DEF_SERIAL_BAUD):
        self.meter_device_manager = meter_device_manager
        self.label = label
        self.state = DeviceStatus.INIT
        self.last_seen = A_UNKNOWN
        self.when_booted = A_UNKNOWN
        self.free_ram = A_UNKNOWN
        self.last_time_drift = A_UNKNOWN
        self.log_level = A_UNKNOWN
        self.encrypt_key = A_UNKNOWN
        self.network_id = network_id
        self.gateway_id = gateway_id
        self.uuid = network_id + '.' + gateway_id
        self.tx_power = A_UNKNOWN

        self.logger = base.get_logger(logger_name=('gway_' + self.uuid))

        self.message_proc_functions = {}
        self.register_msg_proc_func(gmsg.SMSG_GETTIME_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_SETTIME_ACK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_SETTIME_NACK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_GWSNAP_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_SETGITR_ACK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_SETGITR_NACK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_NODESNAP_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_GETNODESNAP_NACK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_MTRUPDATE_NO_IRMS_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_MTRUPDATE_WITH_IRMS_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_MTRREBASE_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_SETMTRVAL_ACK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_SETMTRVAL_NACK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_SETMTRINT_ACK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_SETMTRINT_NACK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_SETPLED_ACK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_SETPLED_NACK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_NODEDARK_DEFN)
        self.register_msg_proc_func(gmsg.SMSG_GPMSG_DEFN)

        self.serial_port = serial_port
        self.serial_baud = serial_baud
        self.serial_conn = serial.Serial(serial_port, serial_baud, timeout=1, write_timeout=1)
        self.logger.info('Started connection to gateway ' + self.uuid + ' on ' + serial_port + ' at ' + serial_baud + ' baud')
        self.serial_tx_msg_buffer = []
        self.serial_rx_msg_objects = {}
        self.rx_msg_objects_seq = 0

        self.serial_thread = threading.Thread(target=self.rx_serial_msg)
        self.serial_thread.daemon = True  # Daemonize thread
        self.serial_thread.start()  # Start the execution

    def register_msg_proc_func(self, message_definition):
        self.message_proc_functions[message_definition['smsg_type']] = 'proc_msg_' + str.lower(message_definition['smsg_type'])

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------
    #  MESSAGE PROCESSING - TO GATEWAY/NODES (TX)
    # ----------------------------------------------------------------------------------------------------------------------------------------------------------
    #  All requests are responded to and processed asynchronously.

    def get_gateway_snapshot(self):
        #  Requests a dump of the gateway's state.
        self.tx_serial_msg(gmsg.get_gateway_snapshot_msg())


    def set_gateway_inst_tmp_rate(self, node_id, tmp_poll_rate, tmp_poll_period):
        #  temporarily changes polling rate of meternode for gateway instructions (e.g. to send new meter value with minimal delay).
        self.tx_serial_msg(gmsg.set_gateway_inst_tmp_rate_msg(node_id, tmp_poll_rate, tmp_poll_period))


    def get_node_snapshot(self, node_id=254):
        #  Requests a dump of a node's state from the Gateway.
        self.tx_serial_msg(gmsg.get_node_snapshot_msg(node_id))


    def set_gateway_time(self):
        # Sends request to gateway to set time to server's local time as Unix UTC Epoch.
        self.tx_serial_msg(gmsg.set_gateway_time_msg(arrow.utcnow().timestamp))


    def set_node_gw_inst_tmp_rate(self, node_id, tmp_ginr_poll_rate, tmp_ginr_poll_time):
        #  Requests a temporary increase of a node's meter GINR rate
        self.tx_serial_msg(gmsg.set_gw_inst_tmp_rate_msg(node_id, tmp_ginr_poll_rate, tmp_ginr_poll_time))


    def set_node_meter_value(self, node_id, new_meter_value):
        #  Requests a reset of a node's meter value to the value specified.
        self.tx_serial_msg(gmsg.set_node_meter_value_msg(node_id, new_meter_value))


    def set_node_meter_interval(self, node_id, new_meter_interval):
        #  Requests a change of a node's metering interval to the value specified.  The interval is the period in seconds at which read entries are created
        #  i.e. (resolution).
        self.tx_serial_msg(gmsg.set_node_meter_interval_msg(node_id, new_meter_interval))


    def set_node_puck_led(self, node_id, new_puck_led_rate, new_puck_led_time):
        #  Requests a change of a node's puck LED rate and time.
        self.tx_serial_msg(gmsg.set_node_puck_led_msg(node_id, new_puck_led_rate, new_puck_led_time))


    def send_gp_msg(self, node_id, message):
        self.tx_serial_msg(gmsg.general_purpose_msg(node_id, message))


    # ----------------------------------------------------------------------------------------------------------------------------------------------------------
    #  MESSAGE PROCESSING - FROM GATEWAY/NODES (RX)
    # ----------------------------------------------------------------------------------------------------------------------------------------------------------

    def serial_rx_msg_buffer_add(self, msg_obj):
        self.rx_msg_objects_seq += 1
        msg_obj['network_id'] = self.network_id
        msg_obj['gateway_id'] = self.gateway_id
        self.serial_rx_msg_objects[str(arrow.utcnow().timestamp) + '/' + str(self.rx_msg_objects_seq)] = msg_obj    # '/' is < 9


    def serial_rx_msg_buffer_purge(self, secs_old):
        purge_before = str(arrow.utcnow().shift(seconds=-secs_old).timestamp)
        self.serial_rx_msg_objects = {key: val for key, val in self.serial_rx_msg_objects.items() if key < purge_before}


    def proc_msg_gtime(self, msg_obj):
        # Process 'get time' request from Gateway, returning a SETTIME
        self.logger.debug("Got time request from gateway {0}.{1}".format(self.network_id, self.gateway_id))
        self.set_gateway_time()


    def proc_msg_stime_ack(self, msg_obj):
        self.logger.debug("Set time for gateway {0}.{1}".format(self.network_id, self.gateway_id))


    def proc_msg_stime_nack(self, msg_obj):
        self.logger.warn("Failed to set time for gateway {0}.{1}".format(self.network_id, self.gateway_id))


    def proc_msg_gwsnap(self, msg_obj):
        # Process gateway dump/snapshot
        self.logger.debug("Got gateway snapshot: {0}".format(msg_obj))
        rec = msg_obj['HEADER_1']
        self.network_id = rec.network_id
        self.gateway_id = rec.gateway_id
        self.uuid = rec.network_id + '.' + rec.gateway_id
        self.when_booted = rec.when_booted
        self.free_ram = rec.free_ram
        self.last_time_drift = arrow.utcnow().timestamp - int(rec.gateway_time)
        self.log_level = rec.log_level
        self.encrypt_key = rec.encrypt_key
        self.tx_power = rec.tx_power
        self.serial_rx_msg_buffer_add(msg_obj)


    def proc_msg_nosnap(self, msg_obj):
        # Process node dump/snapshot.
        self.logger.debug("Got node snapshot(s): {0}".format(msg_obj))
        self.serial_rx_msg_buffer_add(msg_obj)


    def proc_msg_nosnap_nack(self, msg_obj):
        rec = msg_obj['HEADER_1']
        self.logger.warn("Failed to get node snapshot for node {0}.{1}".format(self.network_id, rec.node_id))


    def proc_msg_mup_(self, msg_obj):
        # Process node meter update event from Gateway, creating a meter update object to pass to meter device manager
        self.logger.debug("Got meter update (network={0}): {1}".format(self.network_id, msg_obj))
        self.serial_rx_msg_buffer_add(msg_obj)


    def proc_msg_mupc(self, msg_obj):
        # Process node meter update event from Gateway, creating a meter update object to pass to meter device manager
        self.logger.debug("Got meter update with IRMS (network={0}): {1}".format(self.network_id, msg_obj))
        self.serial_rx_msg_buffer_add(msg_obj)


    def proc_msg_mreb(self, msg_obj):
        # Process node meter rebase event from Gateway
        self.logger.debug("Got meter rebase (network={0}): {1}".format(self.network_id, msg_obj))
        self.serial_rx_msg_buffer_add(msg_obj)


    def proc_msg_sgitr_ack(self, msg_obj):
        rec = msg_obj['HEADER_1']
        self.logger.debug("Set meter GINR rate for node {0}.{1}".format(self.network_id, rec.node_id))


    def proc_msg_sgitr_nack(self, msg_obj):
        rec = msg_obj['HEADER_1']
        self.logger.warn("Failed to set meter GINR rate for node {0}.{1}".format(self.network_id, rec.node_id))


    def proc_msg_smval_ack(self, msg_obj):
        rec = msg_obj['HEADER_1']
        self.logger.debug("Set meter value for node {0}.{1}".format(self.network_id, rec.node_id))


    def proc_msg_smval_nack(self, msg_obj):
        rec = msg_obj['HEADER_1']
        self.logger.warn("Failed to set meter value for node {0}.{1}".format(self.network_id, rec.node_id))


    def proc_msg_spled_ack(self, msg_obj):
        rec = msg_obj['HEADER_1']
        self.logger.debug("Set puck LED for node {0}.{1}".format(self.network_id, rec.node_id))


    def proc_msg_spled_nack(self, msg_obj):
        rec = msg_obj['HEADER_1']
        self.logger.warn("Failed to set meter value for node {0}.{1}".format(self.network_id, rec.node_id))


    def proc_msg_smint_ack(self, msg_obj):
        rec = msg_obj['HEADER_1']
        self.logger.debug("Set meter interval for node {0}.{1}".format(self.network_id, rec.node_id))


    def proc_msg_smint_nack(self, msg_obj):
        rec = msg_obj['HEADER_1']
        self.logger.warn("Failed to set meter value for node {0}.{1}".format(self.network_id, rec.node_id))


    def proc_msg_ndark(self, msg_obj):
        # Process node dark message - indicating that gateway hasn't received proof of life from node
        rec = msg_obj['HEADER_1']
        self.logger.debug("Got node dark notification for node {0}.{1}".format(self.network_id, rec.node_id))
        self.serial_rx_msg_buffer_add(msg_obj)


    def proc_msg_gmsg(self, msg_obj):
        # Process node broadcast event from Gateway
        rec = msg_obj['HEADER_1']
        self.logger.debug("Got general-purpose message from gateway: {0}".format(msg_obj))
        self.serial_rx_msg_buffer_add(msg_obj)


    def tx_serial_msg(self, message):
        message = gmsg.SMSG_TX_PREFIX + message + '\r\n'
        self.serial_tx_msg_buffer.append(message)


    def rx_serial_msg(self):
        loop_count = 0
        while self.serial_conn.isOpen():
            try:
                # read and dispatch inbound lines from serial buffer to appropriate handler

                loop_count = loop_count + 1 if loop_count < 60 else 1

                if self.serial_conn.inWaiting() > 0:
                    serial_in = self.serial_conn.readline().strip().decode("latin1")

                    if serial_in.startswith(gmsg.SMSG_RX_PREFIX):
                        self.logger.debug('Got serial data: %s', serial_in)
                        self.last_seen = arrow.utcnow().timestamp
                        # inbound serial line is a message, so drop prefix and convert it from CSV to message object
                        msg_obj = gmsg.get_message_obj(serial_in, self.uuid, self.gateway_id, self.network_id)

                        # pass object to appropriate processor function using dictionary mapping
                        getattr(self, self.message_proc_functions[msg_obj['message_type']])(msg_obj)

                if len(self.serial_tx_msg_buffer) > 0:
                    tx_msg = self.serial_tx_msg_buffer.pop(0)
                    self.serial_conn.write(tx_msg.encode('utf-8'))
                    self.logger.debug("Wrote serial data: " + tx_msg.strip('\r\n'))

                if loop_count % 30 == 0:
                    self.serial_rx_msg_buffer_purge(PURGE_RX_MSG_AGE_SECS)

                sleep(0.5)

            except serial.serialutil.SerialTimeoutException as err:
                self.logger.debug('Serial timeout: {0}'.format(err))

            except serial.serialutil.SerialException as err:
                self.logger.debug('Serial exception: {0}'.format(err))

            except (KeyboardInterrupt, SystemExit):
                self.serial_conn.close()
                break

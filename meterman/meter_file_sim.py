'''

================================================================================================================================================================
Meter File Simulator
=====================

Command line script that writes simulated meter data records (events and state snapshots) to stdout (can be redirected/piped to serial, file, etc).

Simulates a single meter.  Multiple can be simulated by running multiple instances.

Run with --help for more info.

Note to self - socat syntax: socat -d -d pty,raw,echo=1 pty,raw,echo=1

================================================================================================================================================================

'''

import argparse
from datetime import timedelta
from random import randint
from sys import stdout
from time import sleep

import arrow

from meterman import gateway_messages as gmsg

SERIAL_MAX_ENTRIES = 7

parser = argparse.ArgumentParser(description="Writes simulated meter data records (events and state snapshots) to stdout (can be redirected/piped to serial, "
                                             "file, etc).")
parser.add_argument("--node_id", help="Node Id to simulate (must be between 2 and 254).  Defaults to 2.", type=int, default=2)
parser.add_argument("--network_id", help="Network Id to simulate (octets - 0.0.0.0).  Defaults to 0.0.1.1.", type=str, default="0.0.1.1")
parser.add_argument("--start_entry_id", help="Starting value for sequential entry id. Defaults to 1.", type=int, default=1)
parser.add_argument("--start_val", help="Starting value for accumulation meter in Wh", type=int, default=0)
parser.add_argument("--interval", help="Interval for meter read entries in seconds.  Defaults to 15.", type=int, default=15)
parser.add_argument("--read_min", help="Min value for read generator in Wh.  Defaults to 0.", type=int, default=0)
parser.add_argument("--read_max", help="Max value for read generator in Wh.  Defaults to 10.", type=int, default=10)
parser.add_argument('--serial', help="Output as serial message.  Defaults to false (file).", action='store_true')
parser.add_argument('--events', help="Output random event/snapshot messages.  Defaults to false.", action='store_true')
parser.add_argument("--event_rate", help="Approx period between random events.", type=int, default=60)

args = parser.parse_args()

node_id = args.node_id
network_id = args.network_id
entry_id = args.start_entry_id
meter_value = args.start_val
interval = args.interval
interval_delta = timedelta(seconds=interval)
event_rate = args.event_rate // interval
read_min = args.read_min
read_max = args.read_max
is_serial = args.serial
is_events = args.events

when_last_read = arrow.Arrow.utcfromtimestamp(0)
when_last_entry = arrow.Arrow.utcfromtimestamp(0)

serial_rts = False

last_serial_entry_id = 0
last_entry_timestamp = 0
last_meter_value = 0
next_serial_entry_id = randint(1, SERIAL_MAX_ENTRIES)
rec_out = ""

# Run until Ctrl+C is pressed
try:
    while True:
        if arrow.utcnow() - when_last_entry >= interval_delta:
            when_last_entry = arrow.utcnow()
            entry_timestamp = when_last_entry.shift(seconds=-(interval)).timestamp        # as interval being written now began n seconds ago
            entry_value = randint(read_min, read_max)
            meter_value += entry_value

            if is_serial:
                if rec_out == "":
                    rec_out = "G>S:MUP_;{},MUP_,{},{}".format(node_id, entry_timestamp, meter_value)
                else:
                    rec_out += ";{},{}".format(entry_timestamp - last_entry_timestamp, meter_value - last_meter_value)

                if entry_id == next_serial_entry_id:
                    rec_out += "\r"
                    serial_rts = True
            else:
                rec_out = "{},{},{},{},{},{},{}".format(network_id, node_id, entry_id, entry_timestamp, entry_value, meter_value, interval)

            if not is_serial or serial_rts:
                print(rec_out)
                stdout.flush()
                last_serial_entry_id = entry_id
                next_serial_entry_id = entry_id + randint(2, SERIAL_MAX_ENTRIES + 1)
                rec_out = ""
                serial_rts = False

            entry_id += 1
            last_entry_timestamp = entry_timestamp
            last_meter_value = meter_value

            # send random serial event message
            if is_serial and is_events and randint(0, event_rate) == event_rate:
                print("G>S:" + gmsg.get_random_gateway_event_msg(node_id) + "\r")
                stdout.flush()

        sleep(0.5)

except (KeyboardInterrupt, SystemExit):
    pass

#!/bin/sh

cd /home/pi/temp
wget https://leehonan.com/tfr/firmware.hex
sudo avrdude -c arduino -p atmega328p -P /dev/ttyAMA0 -b 115200 -U flash:w:/home/pi/temp/firmware.hex

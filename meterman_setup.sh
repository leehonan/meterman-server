#!/bin/sh

echo "Installing meterman and meter gateway..."

apt update
apt install wget minicom sqlite3 avrdude libffi-dev libssl-dev zlib1g-dev build-essential checkinstall libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev
mkdir /home/pi/temp

cd /home/pi/temp
wget https://www.python.org/ftp/python/3.6.0/Python-3.6.0.tgz
tar xzf Python-3.6.0.tgz
cd Python-3.6.0/
./configure && sudo make -j4 && sudo make install

cd /home/pi/temp
wget https://leehonan.com/tfr/meterman-dist.tar.gz
pip3.6 install meterman-dist.tar.gz --upgrade

# cd /home/pi/temp
# wget https://leehonan.com/tfr/meterman-support.tar.gz
# tar -zxf meterman-support.tar.gz
#
# cd /home/pi/temp/meterman-support
# cp pishutdown.py /home/pi/
# chown pi:pi /home/pi/pishutdown.py
# chmod +x /home/pi/pishutdown.py
#
# cp autoreset /usr/bin
# cp avrdude-autoreset /usr/bin
# chmod +x /usr/bin/autoreset
# chmod +x /usr/bin/avrdude-autoreset
# mv /usr/bin/avrdude /usr/bin/avrdude-original
# ln -s /usr/bin/avrdude-autoreset /usr/bin/avrdude
#
# cp update_gw_firmware.sh /home/pi
# chmod +x /home/pi/update_gw_firmware.sh
#
# cp pishutdown.service /lib/systemd/system
# cp meterman.service /lib/systemd/system
# chmod 644 /lib/systemd/system/pishutdown.service
# chmod 644 /lib/systemd/system/meterman.service
# systemctl daemon-reload
# systemctl enable pishutdown.service
# systemctl enable meterman.service
# systemctl start pishutdown.service
# systemctl start meterman.service
#
# cd /home/pi/meterman
#
# echo "Done!  Now edit meterman config file and reboot'."

#!/bin/bash

if [ $(/usr/bin/id -u) -ne 0 ]
then
    echo "Not running as sudo!"
    exit
fi

echo "Fetching prerequisites..."
apt update
apt install --yes wget screen minicom sqlite3 avrdude libffi-dev libssl-dev zlib1g-dev build-essential checkinstall libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev

echo "Done\n"

if [ ! -d /home/pi/temp ]
then
    mkdir /home/pi/temp
fi

if [ ! -f /usr/local/bin/python3.6 ]
then
    echo "Setting up Python 3.6..."
    cd /home/pi/temp
    wget https://www.python.org/ftp/python/3.6.0/Python-3.6.0.tgz
    tar xzvf Python-3.6.0.tgz
    cd Python-3.6.0/
    ./configure && sudo make -j4 && sudo make install
    echo "Done\n"
fi

if [ ! -f /usr/bin/autoreset ]
then
    echo "Setting up gateway firmware tools..."
    cd /home/pi/temp
    wget https://github.com/leehonan/rfm69-pi-gateway/raw/master/src/autoreset
    wget https://github.com/leehonan/rfm69-pi-gateway/raw/master/src/avrdude-autoreset
    wget https://github.com/leehonan/rfm69-pi-gateway/raw/master/firmware.hex
    cp autoreset /usr/bin
    cp avrdude-autoreset /usr/bin
    chmod +x /usr/bin/autoreset
    chmod +x /usr/bin/avrdude-autoreset
    mv /usr/bin/avrdude /usr/bin/avrdude-original
    ln -s /usr/bin/avrdude-autoreset /usr/bin/avrdude
    echo "Done\n"
fi

if [ ! -f /lib/systemd/system/pishutdown.service ]
then
    echo "Setting up gateway shutdown..."
    cd /home/pi/temp
    wget https://github.com/leehonan/rfm69-pi-gateway/raw/master/src/pishutdown.py
    wget https://github.com/leehonan/rfm69-pi-gateway/raw/master/src/pishutdown.service
    cp pishutdown.py /home/pi/
    chown pi:pi /home/pi/pishutdown.py
    chmod +x /home/pi/pishutdown.py
    cp pishutdown.service /lib/systemd/system
    chmod 644 /lib/systemd/system/pishutdown.service
    systemctl daemon-reload
    systemctl enable pishutdown.service
    systemctl start pishutdown.service
    echo "Done\n"
fi

echo "Setting up meterman..."
cd /home/pi/temp
wget https://github.com/leehonan/meterman-server/raw/master/meterman/meterman.service
wget https://github.com/leehonan/meterman-server/raw/master/meterman-0.1.tar.gz
pip3.6 install meterman-0.1.tar.gz --upgrade
cp meterman.service /lib/systemd/system
chmod 644 /lib/systemd/system/meterman.service
systemctl daemon-reload
systemctl enable meterman.service
systemctl start meterman.service

echo "Done!  Now edit /home/pi/meterman/config.txt file, configure gateway, and restart"

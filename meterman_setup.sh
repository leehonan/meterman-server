#!/bin/bash

# download with 'sudo wget https://github.com/leehonan/meterman-server/raw/master/meterman_setup.sh'
# then 'sudo chmod +x ./meterman_setup.sh'
# then 'sudo ./meterman_setup.sh'

if [ $(/usr/bin/id -u) -ne 0 ]
then
    echo "Not running as sudo!"
    exit
fi

if [ -e /lib/systemd/system/meterman.service ]
then
    sudo systemctl stop meterman.service
fi

echo "Cleaning up from previous runs..."
rm /home/pi/temp/*.service*
rm /home/pi/temp/autoreset*
rm /home/pi/temp/avrdude*
rm /home/pi/temp/firmware*
rm /home/pi/temp/meterman*
rm /home/pi/temp/pishutdown*
rm -R /home/pi/temp/Python*

echo "Fetching prerequisites..."
apt update
apt install --yes wget screen minicom sqlite3 avrdude libffi-dev libssl-dev zlib1g-dev build-essential checkinstall libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev

echo "Done\n"

if [ ! -e /home/pi/temp ]
then
    mkdir /home/pi/temp
fi

if [ ! -e /usr/local/bin/python3.6 ]
then
    echo "Setting up Python 3.6..."
    cd /home/pi/temp
    wget https://www.python.org/ftp/python/3.6.0/Python-3.6.0.tgz
    tar xzvf Python-3.6.0.tgz
    cd Python-3.6.0/
    ./configure && sudo make -j4 && sudo make install
    echo "Done\n"
fi

if [ ! -e /usr/bin/autoreset ]
then

    echo "Configuring GPIO Serial..."
    if [ ! -e /dev/ttyS0 ]
    then
        sudo systemctl stop serial-getty@ttyAMA0.service
        sudo systemctl disable serial-getty@ttyAMA0.service
    else
        sudo systemctl stop serial-getty@ttyS0.service
        sudo systemctl disable serial-getty@ttyS0.service
    fi

    grep -q -F 'enable_uart=1' /boot/config.txt || echo 'enable_uart=1' >> /boot/config.txt
    sed -e s/console=serial0,115200//g -i /boot/cmdline.txt

    echo "Setting up gateway firmware tools..."
    cd /home/pi/temp
    wget https://github.com/leehonan/rfm69-pi-gateway/raw/master/src/autoreset
    wget https://github.com/leehonan/rfm69-pi-gateway/raw/master/src/avrdude-autoreset
    wget https://github.com/leehonan/rfm69-pi-gateway/raw/master/firmware.hex
    cp firmware.hex /home/pi/
    cp autoreset /usr/bin/
    cp avrdude-autoreset /usr/bin/
    chmod +x /usr/bin/autoreset
    chmod +x /usr/bin/avrdude-autoreset
    mv /usr/bin/avrdude /usr/bin/avrdude-original
    ln -s /usr/bin/avrdude-autoreset /usr/bin/avrdude

    echo "Done\n"
fi

if [ ! -e /lib/systemd/system/pishutdown.service ]
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

echo "Done!  Now..."
echo "  1) reboot if first install for GPIO serial to work"
echo "  2) stop service with 'sudo systemctl stop meterman.service'"
echo "  2) Update gateway with... 'sudo avrdude -c arduino -p atmega328p -P /dev/serial0 -b 115200 -U flash:w:/home/pi/firmware.hex'"
echo "  3) configure gateway with minicom (sudo minicom -b 115200 -o -D /dev/serial0), set neti"
echo "  4) edit /home/pi/meterman/config.txt file"
echo "  5) reboot again"

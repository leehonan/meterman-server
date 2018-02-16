#!/bin/bash

# download with 'sudo wget https://github.com/leehonan/meterman-server/raw/master/scratch/prep_server.sh'
# then 'sudo chmod +x ./prep_server.sh'
# then 'sudo ./prep_server.sh -n <hostname> -p <aws_ssh_port>'

# TODO: autossh, pyramid stuff, hide trashcan if it matters

echo "Preparing server..."

if [ $(/usr/bin/id -u) -ne 0 ]
then
    echo "Not running as sudo!"
    exit
fi

while getopts ":n:p" opt; do
  case $opt in
    n)
      echo "setting hostname to $OPTARG" >&2
      echo $OPTARG | sudo tee /etc/hostname > /dev/null
      echo "127.0.0.1 localhost " $OPTARG | sudo tee /etc/hosts > /dev/null
      ;;
  n)
    echo "setting ssh server port to $OPTARG" >&2
    server_port=$OPTARG
    ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

echo "Ensuring modem is down..."
ifconfig eth1 down
ifconfig wlan0 up

echo "Prioritising wlan0 over eth1..."
route delete default
route add default gateway 192.168.2.1

apt update
apt dist-upgrade --yes --force-yes
apt install --yes --force-yes autossh mongodb wget virtualenv unison=2.40.102-2 screen realvnc-vnc-server python-dev

echo "Setting up VNC..."
vncinitconfig -install-defaults
systemctl enable vncserver-x11-serviced.service
systemctl start vncserver-x11-serviced.service

echo "Disabling Screen Saver..."
echo '' >>  /etc/lightdm/lightdm.conf
echo '# Added by PS setup' >>  /etc/lightdm/lightdm.conf
echo '[Seat:0]' >>  /etc/lightdm/lightdm.conf
echo 'xserver-command=X -s 0 -dpms' >>  /etc/lightdm/lightdm.conf

echo "Hiding Panel..."
sed -i 's/autohide=0/autohide=1/g' -i /home/pi/.config/lxpanel/LXDE-pi/panels/panel
sed -i 's/heightwhenhidden=2/heightwhenhidden=0/g' -i /home/pi/.config/lxpanel/LXDE-pi/panels/panel

echo "Installing meterman..."
cd /home/pi
wget https://github.com/leehonan/meterman-server/raw/master/meterman_setup.sh
chmod +x ./meterman_setup.sh
./meterman_setup.sh -p

echo "setting up autossh..."
mv /Volumes/boot/ps-ssh-server.pem /home/pi/.ssh/
crontab -l > mycron
echo '* * * * * pi /usr/bin/screen -S reverse-ssh-tunnel -d -m autossh -M 20000 -N -f -o "PubkeyAuthentication=yes" -o "PasswordAuthentication=no" -i /home/pi/.ssh/ps-ssh-server.pem -o "ServerAliveInterval 30" -o "ServerAliveCountMax 3" -R $server_port:localhost:22 ubuntu@ec2-52-14-172-128.us-east-2.compute.amazonaws.com' >> mycron
crontab mycron
rm mycron

echo "Running passwd in 5s - change pi's password"
sleep 5
passwd pi

echo "Running dpkg in 5s - set timezone"
sleep 5
dpkg-reconfigure tzdata

echo "Running dpkg in 5s - set keyboard to US"
sleep 5
dpkg-reconfigure keyboard-configuration


echo "Done!  Rebooting in 30s..."

printf "\n==================================================================\n"
echo "AFTER REBOOT FOLLOW INSTRUCTIONS IN /home/pi/readme.txt"
printf "\n==================================================================\n"

echo "Post prep_server.sh instructions..." > /home/pi/readme.txt
echo "" > /home/pi/readme.txt
echo "METERMAN" >> /home/pi/readme.txt
echo "stop meterman service with 'sudo systemctl stop meterman.service'" >> /home/pi/readme.txt
echo "Update gateway with... 'sudo avrdude -c arduino -p atmega328p -P /dev/serial0 -b 115200 -U flash:w:/home/pi/firmware.hex'" >> /home/pi/readme.txt
echo "Configure gateway with... 'sudo minicom -b 115200 -o -D /dev/serial0'... then set neti" >> /home/pi/readme.txt
echo "edit /home/pi/meterman/config.txt file" >> /home/pi/readme.txt
echo "" > /home/pi/readme.txt
echo "MODEM" >> /home/pi/readme.txt
echo "Ensure modem is connected with SIM installed" >> /home/pi/readme.txt
echo "Browse to http://192.168.1.1 set apn to telstra.internet.  Also on Mobile Connection page make sure the network is set to Auto." >> /home/pi/readme.txt
echo "Bounce modem with sudo ifconfig eth1 down; sudo ifconfig eth1 up" >> /home/pi/readme.txt
echo "" > /home/pi/readme.txt
echo "WIFI" >> /home/pi/readme.txt
echo "If desirable uncomment '#dtoverlay=pi3-disable-wifi' in /boot/config.txt to disable wifi" >> /home/pi/readme.txt
echo "" > /home/pi/readme.txt
echo "Now reboot" >> /home/pi/readme.txt

sleep 30
reboot

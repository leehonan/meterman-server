#!/bin/bash

# download with 'sudo wget https://github.com/leehonan/meterman-server/raw/master/scratch/prep_sd.sh'
# then 'sudo chmod +x ./prep_sd.sh'
# then 'sudo ./prep_sd.sh'
# ENSURE AWS SSH key in /Temp/ps-ssh-server.pem

printf "ASSUMING OSX!\n\n"

if [ $(/usr/bin/id -u) -ne 0 ]
then
    echo "Not running as sudo! Exiting..."
    exit
fi

printf "\nPreparing SD (Ctrl-T to see progress):\n"
diskutil list /dev/disk2
diskutil unmountDisk /dev/disk2
printf "\nWriting image (Ctrl-T to see progress):\n"
dd if=/Temp/2017-07-05-raspbian-jessie.img of=/dev/rdisk2 bs=64k conv=sync

printf "\nWaiting for disk to come up..."
sleep 10

printf "\nSetting up SSH"
touch /Volumes/boot/ssh

printf "\nCopying AWS Key"
cp /Temp/ps-ssh-server.pem /Volumes/boot/

printf "\nSetting config.txt entries...\n"
echo '' >> /Volumes/boot/config.txt
echo '# Added by PS setup' >> /Volumes/boot/config.txt
echo 'max_usb_current=1' >> /Volumes/boot/config.txt
echo 'hdmi_group=2' >> /Volumes/boot/config.txt
echo 'hdmi_mode=87' >> /Volumes/boot/config.txt
echo 'hdmi_cvt 800 480 60 6 0 0 0' >> /Volumes/boot/config.txt
echo 'hdmi_drive=1' >> /Volumes/boot/config.txt
echo '#dtoverlay=pi3-disable-wifi' >> /Volumes/boot/config.txt
echo 'dtoverlay=pi3-disable-bt' >> /Volumes/boot/config.txt

printf "\nSetting up wifi...\n"

echo '' >> /Volumes/boot/wpa_supplicant.conf
echo '# Added by PS setup' >> /Volumes/boot/wpa_supplicant.conf
echo 'ctrl_interface=DIR=/var/run/wpa_supplicant GROUP=netdev' >> /Volumes/boot/wpa_supplicant.conf
echo 'update_config=1' >> /Volumes/boot/wpa_supplicant.conf
echo 'country=AU' >> /Volumes/boot/wpa_supplicant.conf
echo 'network={' >> /Volumes/boot/wpa_supplicant.conf
echo 'ssid="CIRCUS_2_4"' >> /Volumes/boot/wpa_supplicant.conf
echo 'psk="insolent-eyewash"' >> /Volumes/boot/wpa_supplicant.conf
echo 'key_mgmt=WPA-PSK' >> /Volumes/boot/wpa_supplicant.conf
echo '}' >> /Volumes/boot/wpa_supplicant.conf

diskutil unmountDisk /dev/disk2

printf "\nDONE!\n"

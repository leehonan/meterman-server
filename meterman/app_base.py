import configparser
import datetime
import random
import string
import logging
import logging.handlers
import os, sys, pwd, shutil
import socket
import arrow

DEVICE_OFF = 0

MIN_TIME = arrow.get(1483228800).timestamp        # Jan 1, 2000
MAX_TIME = arrow.get(datetime.datetime.max).timestamp
DEFAULT_CONFIG_FILE = 'default_config.txt'
CONFIG_FILE = 'config.txt'
DEFAULT_APP_LOGGER = 'base'

# paths set to defaults - should be overridden except for testing
home_path = '/tmp'
temp_path = '/tmp'
log_file = temp_path + '/mm_log_file'
db_file = temp_path + '/mm_db_file'
log_level = 'DEBUG'
config = None

def do_app_init():
    # called by application on init
    working_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    os.chdir(working_dir)

    if not os.path.isfile(CONFIG_FILE):
        shutil.copy(DEFAULT_CONFIG_FILE, CONFIG_FILE)

    global config
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    app_config = config['App']

    global home_path, temp_path, log_file, db_file, log_level
    home_path = app_config['home_path']
    temp_path = app_config['temp_path']

    # create directories if they don't exist
    if not os.path.exists(home_path):
        os.mkdir(home_path)
    if not os.path.exists(temp_path):
        os.mkdir(temp_path)

    config_symlink = home_path + '/' + CONFIG_FILE
    if not os.path.islink(config_symlink):
        os.symlink(working_dir + '/' + CONFIG_FILE, config_symlink)

    log_file = home_path + app_config['log_file']
    db_file = home_path + app_config['db_file']
    log_level = app_config['log_level'].upper()


def get_logger(logger_name=DEFAULT_APP_LOGGER, log_file=log_file, is_msg_out=False):
    # Set up a specific logger with our desired output level
    logger = logging.getLogger(logger_name)

    # Add the log message handlers to the logger
    # create formatter and add it to the handlers
    if is_msg_out:
        file_handler = logging.handlers.TimedRotatingFileHandler(
            log_file, when='D', backupCount=30)  # set log file size and number of logs retained
        formatter = logging.Formatter('%(message)s', '')
        logger.setLevel(logging.DEBUG)
    else:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10000000, backupCount=5)  # set log file size and number of logs retained
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s','%Y-%m-%d %H:%M:%S')
        logger.setLevel(log_level)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info('Initiated logger ' + logger_name + '.  Log level = ' + logging.getLevelName(logger.getEffectiveLevel()))

    return logger


def get_nonce():
    return "".join(random.choice(string.ascii_uppercase) for i in range(2))


def get_log_lines(num_lines):
    '''
    Get n lines from current log file,
    '''
    log_file_r = open(log_file, "r")

    last_line = sum(1 for line in log_file_r) - 1

    if last_line <= num_lines:
        first_line = 0
    else:
        first_line = (last_line - num_lines) + 1

    i = 0
    result = ""

    log_file_r.seek(0, 0)
    for line in log_file_r:
        if first_line <= i <= last_line:
            result += line
        i += 1

    log_file_r.close()
    return result


def get_ip_address():
    # TODO: not working?
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

def get_user():
    return pwd.getpwuid(os.getuid())[0]



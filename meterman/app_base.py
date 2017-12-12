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

working_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
os.chdir(working_dir)
DEFAULT_CONFIG = 'default_config.txt'
config_file = 'config.txt'
if not os.path.isfile(config_file):
    shutil.copy(DEFAULT_CONFIG, config_file)

config = configparser.ConfigParser()
config.read('config.txt')
app_config = config['App']
HOME_PATH = app_config['home_path']
TEMP_PATH = app_config['temp_path']

# create directories if they don't exist
if not os.path.exists(HOME_PATH):
    os.mkdir(HOME_PATH)
if not os.path.exists(TEMP_PATH):
    os.mkdir(TEMP_PATH)

config_symlink = HOME_PATH + '/' + config_file
if not os.path.islink(config_symlink):
    os.symlink(working_dir + '/' + config_file, config_symlink)

# STATE_FILE = HOME_PATH + app_config['state_file']
LOG_FILE = HOME_PATH + app_config['log_file']
DB_FILE = HOME_PATH + app_config['db_file']
LOG_LEVEL = app_config['log_level'].upper()
DEFAULT_APP_LOGGER = 'Meterman'

# TODO, why is working fine on jessie but only one logger writing on stretch?

def get_logger(logger_name=DEFAULT_APP_LOGGER, log_file=LOG_FILE, is_msg_out=False):
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
        logger.setLevel(LOG_LEVEL)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def get_nonce():
    return "".join(random.choice(string.ascii_uppercase) for i in range(2))


def get_log_lines(num_lines):
    '''
    Get n lines from current log file,
    '''
    log_file = open(LOG_FILE, "r")

    last_line = sum(1 for line in log_file) - 1

    if last_line <= num_lines:
        first_line = 0
    else:
        first_line = (last_line - num_lines) + 1

    i = 0
    result = ""

    log_file.seek(0, 0)
    for line in log_file:
        if first_line <= i <= last_line:
            result += line
        i += 1

    log_file.close()
    return result


def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]


logger = get_logger()
logger.info('Running as user: ' + pwd.getpwuid(os.getuid())[0])

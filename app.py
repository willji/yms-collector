import os
import sys
import json
from logging.config import fileConfig
from collector import CollectorManager
from collector.utils import get_home

if __name__ == '__main__':
    if os.path.exists(os.path.join(get_home(), 'logging.ini')):
        fileConfig(os.path.join(get_home(), 'logging.ini'))

    config_file = sys.argv[1] if len(sys.argv) > 1 else os.path.join(get_home(), 'config.json')
    config = {}
    with open(config_file) as f:
        config = json.loads(f.read())
    cm = CollectorManager(config, get_home())
    try:
        cm.start()
    except KeyboardInterrupt:
        cm.shutdown()

import os
import sys


def get_home():
    return os.path.abspath(os.path.dirname(sys.argv[0]))
import os
import json

def save_checkpoint(path, offset):
    with open(path, 'w') as f:
        json.dump({'resultOffset': offset}, f)

def load_checkpoint(path):
    return 0 
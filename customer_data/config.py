import sys
import yaml

def load_config(path):
    with open(path) as f:
        cfg = yaml.safe_load(f)
    api_type = cfg.get('api_type', None)
    if api_type not in ['wayne_ky', 'tulsa'] and 'url' not in cfg:
        sys.exit(1)
    if 'primary_key' not in cfg:
        cfg['primary_key'] = ['OBJECTID']
    if 'deduplicate' not in cfg:
        cfg['deduplicate'] = False
    if 'owners' not in cfg:
        cfg['owners'] = False
    if 'output' not in cfg:
        cfg['output'] = {}
    if 'geopackage' not in cfg['output']:
        cfg['output']['geopackage'] = None
    if 'postgres' not in cfg['output']:
        cfg['output']['postgres'] = {}
    if 'dsn' not in cfg['output']['postgres']:
        cfg['output']['postgres']['dsn'] = None
    return cfg 
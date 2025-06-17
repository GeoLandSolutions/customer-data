import requests
from .checkpoint import save_checkpoint, load_checkpoint

def fetch_metadata(url):
    r = requests.get(f'{url}?f=pjson')
    r.raise_for_status()
    return r.json()

def fetch_features(url, out_fields, offset, page_size, out_sr):
    params = {
        'f': 'json',
        'where': '1=1',
        'outFields': ','.join(out_fields),
        'resultOffset': offset,
        'resultRecordCount': page_size,
        'returnGeometry': 'true',
        'outSR': out_sr
    }
    print(f"Fetching features: offset={offset} page_size={page_size}")
    r = requests.get(f'{url}/query', params=params)
    r.raise_for_status()
    return r.json()

def get_total_count(url):
    params = {'f': 'json', 'where': '1=1', 'returnCountOnly': 'true'}
    r = requests.get(f'{url}/query', params=params)
    r.raise_for_status()
    count = r.json().get('count', None)
    print(f"Total feature count: {count}")
    return count

def extract_all(cfg, checkpoint_file):
    meta = fetch_metadata(cfg['url'])
    sr = meta['extent']['spatialReference'].get('wkid', 4326)
    out_sr = 4326 if sr != 4326 else sr
    fields = [f['name'] for f in meta['fields']]
    page_size = meta.get('maxRecordCount', 1000)
    offset = load_checkpoint(checkpoint_file)
    features = []
    total = get_total_count(cfg['url'])
    print(f"Starting extraction at offset {offset}")
    while True:
        data = fetch_features(cfg['url'], fields, offset, page_size, out_sr)
        fs = data.get('features', [])
        print(f"Fetched {len(fs)} features at offset {offset}")
        if not fs:
            print("No more features returned, stopping.")
            break
        features.extend(fs)
        offset += len(fs)
        save_checkpoint(checkpoint_file, offset)
        if len(fs) < page_size:
            print("Last page fetched (less than page_size), stopping.")
            break
        if total is not None and offset >= total:
            print("Fetched all features (offset >= total), stopping.")
            break
    print(f"Extraction complete. Total features fetched: {len(features)}")
    return meta, features 
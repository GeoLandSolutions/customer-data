import os
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

def fetch_tulsa_data(url, token, last_modified):
    """Fetch data from Tulsa County's custom API"""
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    params = {
        'lastModified': last_modified
    }
    
    print(f"Fetching Tulsa data with lastModified: {last_modified}")
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    print(f"Params: {params}")
    
    r = requests.get(url, headers=headers, params=params)
    
    if not r.ok:
        print(f"API Error: {r.status_code} {r.reason}")
        print(f"Response text: {r.text}")
        r.raise_for_status()
    
    return r.json()

def extract_tulsa(cfg, checkpoint_file):
    """Extract data from Tulsa County's custom API"""
    url = cfg['url']
    token = cfg['token']
    last_modified = cfg.get('last_modified', '01-01-2024')
    
    try:
        from dotenv import load_dotenv
        load_dotenv()  
    except ImportError:
        print("Warning: python-dotenv not installed. Install with: pip install python-dotenv")
    
    if token.startswith('TULSA_ASSESSOR_TOKEN'):
        actual_token = os.getenv(token)
        if actual_token:
            token = actual_token
            print(f"Using token from environment variable: {token[:10]}...")
        else:
            print(f"Warning: Environment variable {token} not found. Using placeholder token.")
    
    print("Starting Tulsa extraction")
    data = fetch_tulsa_data(url, token, last_modified)
    
    print(f"Fetched {len(data)} records from Tulsa API")
    
    return data

def extract_all(cfg, checkpoint_file):
    if cfg.get('api_type') == 'tulsa':
        return extract_tulsa(cfg, checkpoint_file)
    
    # for arcgis extraction
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
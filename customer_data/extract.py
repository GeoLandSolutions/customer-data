import os
import requests
from dotenv import load_dotenv
from .checkpoint import save_checkpoint, load_checkpoint
import json
from customer_data.etl.bossier_la import BossierETL
from customer_data.etl.wayne_ky import WayneKYETL
from customer_data.etl.tulsa_ok import TulsaOKETL
from customer_data.etl.base import BaseJurisdictionETL


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

def fetch_tulsa_data(url, token, last_modified=None):
    """Fetch data from Tulsa County's custom API"""
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    params = {}
    if last_modified:
        params['lastModified'] = last_modified
    
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
    data_type = cfg.get('data_type')
    if 'last_modified' in cfg:
        last_modified = cfg['last_modified']
    else:
        last_modified = None
    if data_type == 'values' and 'last_modified' not in cfg:
        last_modified = None
    
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

def fetch_pvdnet_endpoint(api_base_url, token, endpoint, method="GET", params=None, data=None):
    import json
    headers = {"AccessToken": token, "Content-Type": "application/json"}
    url = f"{api_base_url}/{endpoint.lstrip('/')}"
    if method.upper() == "GET":
        resp = requests.get(url, headers=headers, params=params)
    elif method.upper() == "POST":
        resp = requests.post(url, headers=headers, json=data)
    else:
        raise ValueError(f"Unsupported method: {method}")
    resp.raise_for_status()
    try:
        return json.loads(resp.content.decode("utf-8-sig"))
    except Exception as e:
        print(f"Failed to decode JSON from {endpoint}. Status: {resp.status_code}")
        print(f"Response text: {resp.text}")
        raise


def search_parcels(api_base_url, token, year, output_path, **criteria):
    # This endpoint path is a guess based on the API model
    endpoint = "/pvdnetapi/v1/parcel/search"
    search_criteria = {"year": year}
    search_criteria.update(criteria)
    print(f"Searching parcels with criteria: {search_criteria}")
    results = fetch_pvdnet_endpoint(api_base_url, token, endpoint, method="POST", data=search_criteria)
    print(f"Saving parcel search results to {output_path}")
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    return results

def fetch_adhoc_tables(api_base_url, token, output_path):
    endpoint = "/adhoc/tables"
    headers = {"AccessToken": token}
    url = f"{api_base_url}{endpoint}"
    print(f"Fetching Adhoc tables from {url}")
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    import json
    tables = json.loads(resp.content.decode("utf-8-sig"))
    with open(output_path, "w") as f:
        json.dump(tables, f, indent=2)
    print(f"Saved Adhoc tables to {output_path}")
    return tables


def run_adhoc_query(api_base_url, token, query, output_path):
    endpoint = "/adhoc/tables/query"
    headers = {"AccessToken": token, "Content-Type": "application/json"}
    url = f"{api_base_url}{endpoint}"
    payload = {"Query": query}
    print(f"Running Adhoc query: {query}")
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    import json
    results = json.loads(resp.content.decode("utf-8-sig"))
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Saved Adhoc query results to {output_path}")
    return results

def extract_all_adhoc_tables(cfg, api_base_url, token, tables_json_path, output_dir):
    import json
    import os
    print(f"Loading table list from {tables_json_path}")
    print(f"Current working directory: {os.getcwd()}")
    with open(tables_json_path) as f:
        tables_info = json.load(f)
    tables = tables_info.get("tables", [])
    os.makedirs(output_dir, exist_ok=True)
    for table in tables:
        table_name = table["name"]
        print(f"Extracting all data from table: {table_name}")
        query = f"SELECT * FROM {table_name}"
        output_path = os.path.join(output_dir, f"wayne_ky_{table_name}.json")
        print(f"Writing to: {output_path}")
        try:
            run_adhoc_query(api_base_url, token, query, output_path)
            print(f"File exists after write? {os.path.exists(output_path)}")
        except Exception as e:
            print(f"Failed to extract table {table_name}: {e}")

def get_etl_class(api_type):
    if api_type == 'wayne_ky':
        return WayneKYETL
    if api_type == 'tulsa':
        return TulsaOKETL
    if api_type == 'bossier':
        return BossierETL
    raise ValueError(f"Unsupported api_type: {api_type}")

def extract_all(cfg, checkpoint_file):
    api_type = cfg.get('api_type')
    etl_cls = get_etl_class(api_type)
    etl = etl_cls(cfg)
    return etl.extract(checkpoint_file) 
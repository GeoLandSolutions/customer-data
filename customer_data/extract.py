import os
import requests
from dotenv import load_dotenv
from .checkpoint import save_checkpoint, load_checkpoint
import json


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

def extract_wayne_ky(cfg, checkpoint_file):
    """Authenticate to Wayne, KY PVDNet API, print the access token and resource groups, fetch Adhoc tables, run a sample Adhoc query, and optionally extract all tables."""
    load_dotenv()
    api_base_url = cfg['api_base_url']
    username = os.getenv(cfg['username_env'])
    password = os.getenv(cfg['password_env'])
    if not username or not password:
        raise ValueError(f"Missing credentials: username={username}, password={'set' if password else 'unset'}")
    auth_url = f"{api_base_url}/authenticate"
    payload = {"username": username, "password": password}
    headers = {"Content-Type": "application/json"}
    print(f"Authenticating to PVDNet API at {auth_url}...")
    r = requests.post(auth_url, json=payload, headers=headers)
    try:
        data = json.loads(r.content.decode("utf-8-sig"))
        token = data.get("token")
        resource_groups = data.get("resourceGroups")
    except Exception as e:
        print(f"Failed to decode JSON from response. Status: {r.status_code}")
        print(f"Response text: {r.text}")
        raise
    print("\n================ ACCESS TOKEN ================" )
    print(token)
    print("============================================\n")
    print("Resource Groups:")
    print(resource_groups)
    print("\nCopy this token and use it in the Swagger UI to explore endpoints.")
    # Set new output directory structure
    base_dir = os.path.join("output", "ky", "wayne")
    os.makedirs(base_dir, exist_ok=True)
    tables_output = os.path.join(base_dir, "wayne_ky_adhoc_tables.json")
    tables = fetch_adhoc_tables(api_base_url, token, tables_output)
    # Run a sample Adhoc query if a query is provided in config
    adhoc_query = cfg.get('adhoc_query')
    if adhoc_query:
        query_output = os.path.join(base_dir, "wayne_ky_adhoc_query.json")
        run_adhoc_query(api_base_url, token, adhoc_query, query_output)
    # Extract all tables if requested
    if cfg.get('extract_all_tables'):
        output_dir = os.path.join(base_dir, "all_tables")
        extract_all_adhoc_tables(cfg, api_base_url, token, tables_output, output_dir)
    return {"token": token, "resourceGroups": resource_groups, "tables": tables}

def extract_all(cfg, checkpoint_file):
    if cfg.get('api_type') == 'wayne_ky':
        return extract_wayne_ky(cfg, checkpoint_file)
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
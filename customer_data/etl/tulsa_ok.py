import os
from dotenv import load_dotenv
from customer_data.etl.base import BaseJurisdictionETL
import requests
import json
import csv

def ensure_dir_exists(file_path):
    dir_path = os.path.dirname(file_path)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

class TulsaOKETL(BaseJurisdictionETL):
    def extract(self, checkpoint_file=None):
        cfg = self.cfg
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
        data = self.fetch_tulsa_data(url, token, last_modified)
        print(f"Fetched {len(data)} records from Tulsa API")
        # Write output files
        out = cfg.get('output', {})
        if out.get('json'):
            json_path = out['json']
            ensure_dir_exists(json_path)
            print(f"Writing JSON to {json_path}")
            with open(json_path, 'w') as f:
                json.dump(data, f, indent=2)
        if out.get('csv') and data:
            csv_path = out['csv']
            ensure_dir_exists(csv_path)
            print(f"Writing CSV to {csv_path}")
            all_keys = set()
            for record in data:
                all_keys.update(record.keys())
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
                writer.writeheader()
                writer.writerows(data)
        return data

    def transform(self, data):
        return data

    def load(self, data):
        return data

    def fetch_tulsa_data(self, url, token, last_modified=None):
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
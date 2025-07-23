import os
from dotenv import load_dotenv
from customer_data.etl.base import BaseJurisdictionETL
import requests

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
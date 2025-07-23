import os
import requests
from customer_data.etl.base import BaseJurisdictionETL
from customer_data.checkpoint import save_checkpoint, load_checkpoint
from customer_data.utils import ensure_dir_exists
import json

class BossierETL(BaseJurisdictionETL):
    def extract(self, checkpoint_file=None):
        cfg = self.cfg
        url = cfg['url']
        meta = self.fetch_metadata(url)
        sr = meta['extent']['spatialReference'].get('wkid', 4326)
        out_sr = 4326 if sr != 4326 else sr
        fields = [f['name'] for f in meta['fields']]
        page_size = meta.get('maxRecordCount', 1000)
        offset = load_checkpoint(checkpoint_file)
        features = []
        total = self.get_total_count(url)
        print(f"Starting extraction at offset {offset}")
        while True:
            data = self.fetch_features(url, fields, offset, page_size, out_sr)
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
        # Save output to output/la/bossier/
        base_dir = os.path.join("output", "la", "bossier")
        os.makedirs(base_dir, exist_ok=True)
        meta_path = os.path.join(base_dir, "bossier_meta.json")
        features_path = os.path.join(base_dir, "bossier_features.json")
        ensure_dir_exists(meta_path)
        ensure_dir_exists(features_path)
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        with open(features_path, "w") as f:
            json.dump(features, f, indent=2)
        print(f"Saved meta to {meta_path}")
        print(f"Saved features to {features_path}")
        return meta, features

    def transform(self, data):
        # No-op for now
        return data

    def load(self, data):
        # No-op for now
        return data

    def fetch_metadata(self, url):
        r = requests.get(f'{url}?f=pjson')
        r.raise_for_status()
        return r.json()

    def fetch_features(self, url, out_fields, offset, page_size, out_sr):
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

    def get_total_count(self, url):
        params = {'f': 'json', 'where': '1=1', 'returnCountOnly': 'true'}
        r = requests.get(f'{url}/query', params=params)
        r.raise_for_status()
        count = r.json().get('count', None)
        print(f"Total feature count: {count}")
        return count 
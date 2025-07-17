import sys
import os
import json
import csv
from .config import load_config
from .extract import extract_all, extract_tulsa

def ensure_dir_exists(file_path):
    dir_path = os.path.dirname(file_path)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        print(f"Created directory: {dir_path}")

def handle_tulsa(cfg, last_modified_override=None, data_type=None):
    """Handle Tulsa data extraction and output directly"""
    print("Starting Tulsa extraction")
    if last_modified_override:
        cfg['last_modified'] = last_modified_override
        print(f"Using last_modified override: {last_modified_override}")
    
    # set the URL based on data type selection
    if data_type == "all" and 'url_all' in cfg:
        cfg['url'] = cfg['url_all']
        print("Using 'all' data URL")
        print(f"URL: {cfg['url']}")
    elif data_type == "sales" or data_type is None:
        print("Using 'sales' data URL")
        print(f"URL: {cfg['url']}")
    else:
        print(f"Invalid data_type: {data_type}. Using 'sales' data URL")
        print(f"URL: {cfg['url']}")
    
    data = extract_tulsa(cfg, '.checkpoint')
    out = cfg['output']
    
    # JSON
    if out.get('json'):
        json_path = out['json']
        ensure_dir_exists(json_path)
        print(f"Writing JSON to {json_path}")
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    # CSV 
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

def handle_arcgis(cfg):
    """Handle ArcGIS data extraction and processing"""
    # Import geopandas-dependent modules only when needed
    from .transform import features_to_gdf, deduplicate_gdf, extract_owners
    from .load import write_geopackage, write_postgis
    import pandas as pd
    
    cache_mode = cfg.get('features_cache', 'new')
    features_path = cfg.get('features_path', 'features.json')
    ensure_dir_exists(features_path)
    
    if cache_mode == 'load' and os.path.exists(features_path):
        print(f"Loading meta and features from {features_path}")
        with open(features_path) as f:
            cache = json.load(f)
        meta = cache['meta']
        features = cache['features']
    else:
        meta, features = extract_all(cfg, '.checkpoint')
        print(f"Saving meta and features to {features_path}")
        with open(features_path, 'w') as f:
            json.dump({'meta': meta, 'features': features}, f)
    gdf = features_to_gdf(meta, features)
    if cfg['deduplicate']:
        gdf = deduplicate_gdf(gdf, cfg['primary_key'])
    owners = extract_owners(gdf) if cfg['owners'] else None
    out = cfg['output']
    if out.get('geopackage'):
        ensure_dir_exists(out['geopackage'])
        print("Writing GeoPackage")
        write_geopackage(gdf, owners if owners is not None else pd.DataFrame(), out['geopackage'])
    if out.get('postgres', {}).get('dsn'):
        print("Writing PostGIS")
        write_postgis(gdf, owners, out['postgres']['dsn'])

def main():
    print("Starting main")
    if len(sys.argv) < 2 or len(sys.argv) > 4:
        print("Usage: python -m customer_data <config.yaml> [data_type] [last_modified_date]")
        print("  data_type: Optional - 'sales' or 'all' for Tulsa API (default: 'sales')")
        print("  last_modified_date: Optional date for Tulsa API (MM-DD-YYYY format)")
        sys.exit(1)
    
    cfg = load_config(sys.argv[1])
    data_type = None
    last_modified_override = None
    
    if len(sys.argv) >= 3:
        # check if the second arg is a data_type or last_modified_date
        arg2 = sys.argv[2]
        if arg2 in ['sales', 'all']:
            data_type = arg2
            last_modified_override = sys.argv[3] if len(sys.argv) == 4 else None
        else:
            last_modified_override = arg2
    
    if cfg.get('api_type') == 'tulsa':
        handle_tulsa(cfg, last_modified_override, data_type)
        print("Done")
        sys.exit(0)
    else:
        handle_arcgis(cfg)
        print("Done")
        sys.exit(0)

if __name__ == "__main__":
    main() 
import sys
import os
import json
from .config import load_config
from .extract import extract_all
from .transform import features_to_gdf, deduplicate_gdf, extract_owners
from .load import write_geopackage, write_postgis
import pandas as pd

def main():
    print("Starting main")
    if len(sys.argv) != 2:
        print("Wrong number of arguments")
        sys.exit(1)
    cfg = load_config(sys.argv[1])
    cache_mode = cfg.get('features_cache', 'new')
    features_path = cfg.get('features_path', 'features.json')
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
        print("Writing GeoPackage")
        write_geopackage(gdf, owners if owners is not None else pd.DataFrame(), out['geopackage'])
    if out.get('postgres', {}).get('dsn'):
        print("Writing PostGIS")
        write_postgis(gdf, owners, out['postgres']['dsn'])
    print("Done")
    sys.exit(0)

if __name__ == "__main__":
    main() 
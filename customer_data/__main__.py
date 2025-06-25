import sys
import os
import json
import logging
from pathlib import Path
from .config import load_config
from .extract import extract_all
from .transform import data_to_gdf, deduplicate_gdf, extract_owners
from .load import write_geopackage, write_postgis, write_csv
import pandas as pd

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def load_env_file(logger):
    """Load environment variables from .env file"""
    env_file = Path('.env')
    if env_file.exists():
        logger.info("Loading environment variables from .env file")
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        logger.info("Environment variables loaded successfully")
    else:
        logger.warning(".env file not found. Create one from env.example if you need to store tokens.")

def main():
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("Starting Customer Data Extraction Tool")
    logger.info("=" * 60)
    
    # Load environment variables first
    load_env_file(logger)
    
    if len(sys.argv) != 2:
        logger.error("Wrong number of arguments. Usage: python -m customer_data <config_file>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    logger.info(f"Loading configuration from: {config_file}")
    
    try:
        cfg = load_config(config_file)
        logger.info(f"Configuration loaded successfully")
        logger.info(f"API Type: {cfg.get('api_type', 'unknown')}")
        logger.info(f"Collection Name: {cfg.get('collection_name', 'N/A')}")
        
        if cfg.get('api_type') == 'rest':
            logger.info(f"Number of endpoints: {len(cfg.get('endpoints', []))}")
            for i, endpoint in enumerate(cfg.get('endpoints', []), 1):
                logger.info(f"  Endpoint {i}: {endpoint.get('name', 'Unknown')} - {endpoint.get('method', 'GET')} {endpoint.get('url', 'N/A')}")
        
        cache_mode = cfg.get('features_cache', 'new')
        features_path = cfg.get('features_path', 'features.json')
        
        logger.info(f"Cache mode: {cache_mode}")
        logger.info(f"Features path: {features_path}")
        
        if cache_mode == 'load' and os.path.exists(features_path):
            logger.info(f"Loading cached data from {features_path}")
            with open(features_path) as f:
                cache = json.load(f)
            meta = cache['meta']
            features = cache['features']
            logger.info(f"Loaded {len(features)} cached features")
        else:
            logger.info("Starting data extraction...")
            meta, features = extract_all(cfg, '.checkpoint')
            logger.info(f"Extraction completed. Total records: {len(features)}")
            
            logger.info(f"Saving extracted data to {features_path}")
            with open(features_path, 'w') as f:
                json.dump({'meta': meta, 'features': features}, f)
            logger.info("Data saved successfully")
        
        logger.info("Starting data transformation...")
        data = data_to_gdf(meta, features)
        logger.info(f"Transformation completed. Data shape: {data.shape}")
        logger.info(f"Data type: {type(data).__name__}")
        
        if cfg['deduplicate']:
            logger.info("Performing deduplication...")
            original_count = len(data)
            data = deduplicate_gdf(data, cfg['primary_key'])
            logger.info(f"Deduplication completed. Records: {original_count} -> {len(data)}")
        
        owners = None
        if cfg['owners']:
            logger.info("Extracting owner information...")
            owners = extract_owners(data)
            logger.info(f"Owner extraction completed. Owner records: {len(owners)}")
        
        out = cfg['output']
        logger.info("Starting output generation...")
        
        # Write outputs based on data type and configuration
        if out.get('geopackage'):
            logger.info(f"Writing GeoPackage to: {out['geopackage']}")
            write_geopackage(data, owners if owners is not None else pd.DataFrame(), out['geopackage'])
            logger.info("GeoPackage written successfully")
        
        if out.get('csv'):
            logger.info(f"Writing CSV to: {out['csv']}")
            write_csv(data, owners if owners is not None else pd.DataFrame(), out['csv'])
            logger.info("CSV written successfully")
        
        if out.get('postgres', {}).get('dsn'):
            logger.info("Writing to PostGIS...")
            write_postgis(data, owners, out['postgres']['dsn'])
            logger.info("PostGIS write completed successfully")
        
        logger.info("=" * 60)
        logger.info("Customer Data Extraction Tool completed successfully!")
        logger.info("=" * 60)
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Error during execution: {str(e)}")
        logger.error("Stack trace:", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 
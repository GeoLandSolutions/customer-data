import requests
import logging
from .checkpoint import save_checkpoint, load_checkpoint

logger = logging.getLogger(__name__)

def fetch_metadata(url):
    """Fetch metadata from ArcGIS FeatureServer"""
    logger.info(f"Fetching metadata from: {url}")
    r = requests.get(f'{url}?f=pjson')
    r.raise_for_status()
    metadata = r.json()
    logger.info(f"Metadata fetched successfully. Fields: {len(metadata.get('fields', []))}")
    return metadata

def fetch_features(url, out_fields, offset, page_size, out_sr):
    """Fetch features from ArcGIS FeatureServer"""
    params = {
        'f': 'json',
        'where': '1=1',
        'outFields': ','.join(out_fields),
        'resultOffset': offset,
        'resultRecordCount': page_size,
        'returnGeometry': 'true',
        'outSR': out_sr
    }
    logger.debug(f"Fetching features: offset={offset} page_size={page_size}")
    r = requests.get(f'{url}/query', params=params)
    r.raise_for_status()
    return r.json()

def get_total_count(url):
    """Get total feature count from ArcGIS FeatureServer"""
    logger.info("Getting total feature count...")
    params = {'f': 'json', 'where': '1=1', 'returnCountOnly': 'true'}
    r = requests.get(f'{url}/query', params=params)
    r.raise_for_status()
    count = r.json().get('count', None)
    logger.info(f"Total feature count: {count}")
    return count

def fetch_rest_api_data(endpoint, auth=None):
    """Fetch data from REST API endpoint"""
    headers = {}
    
    # Add authentication headers
    if auth and auth.get('type') == 'bearer' and auth.get('token'):
        headers['Authorization'] = f"Bearer {auth['token']}"
        logger.info("Using Bearer token authentication")
    else:
        logger.info("No authentication required")
    
    # Add custom headers from endpoint
    for header in endpoint.get('headers', []):
        if isinstance(header, dict) and 'key' in header and 'value' in header:
            headers[header['key']] = header['value']
            logger.debug(f"Added custom header: {header['key']}")
    
    logger.info(f"Fetching from REST API: {endpoint['name']}")
    logger.info(f"URL: {endpoint['url']}")
    logger.info(f"Method: {endpoint['method']}")
    
    try:
        r = requests.get(endpoint['url'], headers=headers, timeout=30)
        r.raise_for_status()
        
        data = r.json()
        record_count = len(data) if isinstance(data, list) else 1
        logger.info(f"Successfully received {record_count} records from {endpoint['name']}")
        return data
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout error when fetching from {endpoint['name']}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error when fetching from {endpoint['name']}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error when fetching from {endpoint['name']}: {e}")
        raise

def extract_arcgis_features(cfg, checkpoint_file):
    """Extract features from ArcGIS FeatureServer (existing logic)"""
    logger.info("Starting ArcGIS FeatureServer extraction...")
    
    meta = fetch_metadata(cfg['url'])
    sr = meta['extent']['spatialReference'].get('wkid', 4326)
    out_sr = 4326 if sr != 4326 else sr
    fields = [f['name'] for f in meta['fields']]
    page_size = meta.get('maxRecordCount', 1000)
    
    logger.info(f"Spatial reference: {sr} -> {out_sr}")
    logger.info(f"Fields to extract: {len(fields)}")
    logger.info(f"Page size: {page_size}")
    
    offset = load_checkpoint(checkpoint_file)
    features = []
    total = get_total_count(cfg['url'])
    
    logger.info(f"Starting extraction at offset {offset}")
    
    while True:
        data = fetch_features(cfg['url'], fields, offset, page_size, out_sr)
        fs = data.get('features', [])
        logger.info(f"Fetched {len(fs)} features at offset {offset}")
        
        if not fs:
            logger.info("No more features returned, stopping.")
            break
            
        features.extend(fs)
        offset += len(fs)
        save_checkpoint(checkpoint_file, offset)
        
        if len(fs) < page_size:
            logger.info("Last page fetched (less than page_size), stopping.")
            break
            
        if total is not None and offset >= total:
            logger.info("Fetched all features (offset >= total), stopping.")
            break
    
    logger.info(f"ArcGIS extraction complete. Total features fetched: {len(features)}")
    return meta, features

def extract_rest_api_data(cfg, checkpoint_file):
    """Extract data from REST API endpoints"""
    logger.info("Starting REST API extraction...")
    
    # For REST APIs, we'll collect data from all endpoints
    all_data = []
    metadata = {
        'api_type': 'rest',
        'collection_name': cfg.get('collection_name', 'Unknown'),
        'endpoints': []
    }
    
    endpoints = cfg.get('endpoints', [])
    logger.info(f"Processing {len(endpoints)} endpoints...")
    
    for i, endpoint in enumerate(endpoints, 1):
        logger.info(f"Processing endpoint {i}/{len(endpoints)}: {endpoint['name']}")
        
        try:
            data = fetch_rest_api_data(endpoint, cfg.get('auth'))
            
            # Store endpoint metadata
            endpoint_meta = {
                'name': endpoint['name'],
                'url': endpoint['url'],
                'method': endpoint['method'],
                'record_count': len(data) if isinstance(data, list) else 1
            }
            metadata['endpoints'].append(endpoint_meta)
            
            # Add endpoint name to each record for tracking
            if isinstance(data, list):
                for record in data:
                    record['_endpoint'] = endpoint['name']
                    record['_source_url'] = endpoint['url']
                all_data.extend(data)
                logger.info(f"Added {len(data)} records from {endpoint['name']}")
            else:
                data['_endpoint'] = endpoint['name']
                data['_source_url'] = endpoint['url']
                all_data.append(data)
                logger.info(f"Added 1 record from {endpoint['name']}")
                
        except Exception as e:
            logger.error(f"Error fetching from endpoint {endpoint['name']}: {e}")
            logger.error("Continuing with next endpoint...")
            continue
    
    logger.info(f"REST API extraction complete. Total records fetched: {len(all_data)}")
    logger.info(f"Successful endpoints: {len(metadata['endpoints'])}/{len(endpoints)}")
    
    return metadata, all_data

def extract_all(cfg, checkpoint_file):
    """Main extraction function that handles both ArcGIS and REST APIs"""
    api_type = cfg.get('api_type', 'arcgis')
    logger.info(f"Starting extraction for API type: {api_type}")
    
    if api_type == 'arcgis':
        return extract_arcgis_features(cfg, checkpoint_file)
    elif api_type == 'rest':
        return extract_rest_api_data(cfg, checkpoint_file)
    else:
        raise ValueError(f"Unsupported API type: {api_type}") 
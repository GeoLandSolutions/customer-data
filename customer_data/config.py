import sys
import yaml
import json
import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

def load_config(path):
    """Load configuration from YAML file or Postman collection JSON"""
    # Fix: If path is a list, use the first element
    if isinstance(path, list):
        path = path[0]
    path = Path(path)
    logger.info(f"Loading configuration from: {path}")
    
    if path.suffix.lower() == '.json':
        logger.info("Detected JSON file - treating as Postman collection")
        return load_postman_config(path)
    else:
        logger.info("Detected YAML file - treating as configuration file")
        return load_yaml_config(path)

def get_env_token(token_name):
    """Get token from environment variable with fallback"""
    token = os.getenv(token_name)
    if token:
        logger.info(f"Using token from environment variable: {token_name}")
        return token
    else:
        logger.warning(f"Environment variable {token_name} not found")
        return None

def extract_state_and_jurisdiction_from_path(path):
    """Extract state and jurisdiction from file path"""
    # Ensure path is a string or Path object, not a list
    if isinstance(path, list):
        path = path[0]
    path_obj = Path(path) if not isinstance(path, Path) else path
    path_parts = path_obj.parts
    if len(path_parts) >= 3 and path_parts[-3] in ['la', 'ok', 'tx', 'ca', 'fl', 'ny', 'il', 'pa', 'oh', 'ga']:
        state = path_parts[-3]
        jurisdiction = path_parts[-2]
        return state, jurisdiction
    return None, None

def extract_state_and_jurisdiction_from_name(name):
    """Extract state and jurisdiction from collection name"""
    # Common patterns in collection names
    patterns = [
        r'(tulsa|oklahoma).*assessor',  # Tulsa County Assessor
        r'(bossier|louisiana).*parish',  # Bossier Parish
        r'(\w+)\s+(county|parish|city)',  # Generic county/parish pattern
    ]
    
    name_lower = name.lower()
    
    for pattern in patterns:
        match = re.search(pattern, name_lower)
        if match:
            jurisdiction = match.group(1)
            if 'tulsa' in jurisdiction or 'oklahoma' in jurisdiction:
                return 'ok', 'tulsa'
            elif 'bossier' in jurisdiction or 'louisiana' in jurisdiction:
                return 'la', 'bossier'
            else:
                return 'unknown', jurisdiction
    
    return 'unknown', 'unknown'

def load_postman_config(path):
    """Load configuration from Postman collection JSON"""
    # Ensure path is a string or Path, not a list
    if isinstance(path, list):
        path = path[0]
    logger.info("Parsing Postman collection...")
    
    with open(path) as f:
        collection = json.load(f)
    
    # Extract base configuration from collection
    info = collection.get('info', {})
    variables = {var['key']: var['value'] for var in collection.get('variable', [])}
    auth = collection.get('auth', {})
    
    logger.info(f"Collection name: {info.get('name', 'Unknown')}")
    logger.info(f"Variables found: {list(variables.keys())}")
    logger.info(f"Authentication type: {auth.get('type', 'none')}")
    
    # Check for token in environment variables
    if auth.get('type') == 'bearer':
        # Look for token in environment variables first
        env_token = get_env_token('TULSA_ASSESSOR_TOKEN')
        if env_token:
            variables['token'] = env_token
            logger.info("Using token from environment variable")
        elif variables.get('token') == 'YOUR_ACTUAL_TOKEN_HERE':
            logger.error("Token not found in environment variables. Please set TULSA_ASSESSOR_TOKEN in your .env file")
            logger.error("Example: TULSA_ASSESSOR_TOKEN=your_actual_token_here")
            sys.exit(1)
    
    # Find API endpoints in the collection
    endpoints = []
    for item in collection.get('item', []):
        if 'request' in item:
            request = item['request']
            url = request.get('url', {})
            
            # Build full URL
            if isinstance(url, dict):
                host = url.get('host', [''])
                path = url.get('path', [])
                query = url.get('query', [])
                
                # Get domain from variables
                domain = variables.get('domain', '')
                if domain and not domain.endswith('/'):
                    domain += '/'
                
                # Build path
                full_path = '/'.join(path) if path else ''
                
                # Build query string
                query_params = []
                for param in query:
                    if isinstance(param, dict) and 'key' in param and 'value' in param:
                        query_params.append(f"{param['key']}={param['value']}")
                
                query_string = '&'.join(query_params)
                full_url = f"{domain}{full_path}"
                if query_string:
                    full_url += f"?{query_string}"
                
                endpoints.append({
                    'name': item.get('name', 'Unknown'),
                    'method': request.get('method', 'GET'),
                    'url': full_url,
                    'headers': request.get('header', [])
                })
    
    logger.info(f"Found {len(endpoints)} endpoints in collection")
    for i, endpoint in enumerate(endpoints, 1):
        logger.info(f"  Endpoint {i}: {endpoint['name']} - {endpoint['method']} {endpoint['url']}")
    
    # Determine state and jurisdiction for organized output paths
    state, jurisdiction = extract_state_and_jurisdiction_from_path(path)
    if not state or not jurisdiction:
        state, jurisdiction = extract_state_and_jurisdiction_from_name(info.get('name', ''))
    
    logger.info(f"Detected state: {state}, jurisdiction: {jurisdiction}")
    
    # Create configuration structure
    cfg = {
        'api_type': 'rest',
        'collection_name': info.get('name', 'Unknown Collection'),
        'base_url': variables.get('domain', ''),
        'auth': {
            'type': auth.get('type', 'none'),
            'token': variables.get('token', '') if auth.get('type') == 'bearer' else None
        },
        'endpoints': endpoints,
        'primary_key': ['id'],  # Default primary key
        'deduplicate': False,
        'owners': False,
        'output': {},
        'features_cache': 'new',
        'features_path': f"output/{state}/{jurisdiction}/intermediate/{info.get('name', 'collection').lower().replace(' ', '_')}_features.json"
    }
    
    # Set default output paths
    if 'geopackage' not in cfg['output']:
        cfg['output']['geopackage'] = f"output/{state}/{jurisdiction}/final/{info.get('name', 'collection').lower().replace(' ', '_')}.gpkg"
    if 'csv' not in cfg['output']:
        cfg['output']['csv'] = f"output/{state}/{jurisdiction}/final/{info.get('name', 'collection').lower().replace(' ', '_')}.csv"
    if 'postgres' not in cfg['output']:
        cfg['output']['postgres'] = {}
    if 'dsn' not in cfg['output']['postgres']:
        cfg['output']['postgres']['dsn'] = None
    
    logger.info("Postman collection configuration loaded successfully")
    return cfg

def load_yaml_config(path):
    """Load configuration from YAML file (existing ArcGIS format)"""
    logger.info("Parsing YAML configuration...")
    
    with open(path) as f:
        cfg = yaml.safe_load(f)
    
    # Set API type for backward compatibility
    if 'url' not in cfg:
        logger.error("No 'url' field found in configuration")
        sys.exit(1)
    
    # Determine if this is ArcGIS FeatureServer
    if 'FeatureServer' in cfg['url']:
        cfg['api_type'] = 'arcgis'
        logger.info("Detected ArcGIS FeatureServer configuration")
    else:
        cfg['api_type'] = 'rest'
        logger.info("Detected REST API configuration")
    
    logger.info(f"API URL: {cfg['url']}")
    
    # Check for tokens in environment variables for REST APIs
    if cfg['api_type'] == 'rest' and 'auth' in cfg and cfg['auth'].get('type') == 'bearer':
        if cfg['auth'].get('token') == 'YOUR_ACTUAL_TOKEN_HERE':
            # Look for token in environment variables
            env_token = get_env_token('TULSA_ASSESSOR_TOKEN')
            if env_token:
                cfg['auth']['token'] = env_token
                logger.info("Using token from environment variable")
            else:
                logger.error("Token not found in environment variables. Please set TULSA_ASSESSOR_TOKEN in your .env file")
                logger.error("Example: TULSA_ASSESSOR_TOKEN=your_actual_token_here")
                sys.exit(1)
    
    # Set defaults
    if 'primary_key' not in cfg:
        cfg['primary_key'] = ['OBJECTID']
        logger.info("Using default primary key: ['OBJECTID']")
    else:
        logger.info(f"Primary key: {cfg['primary_key']}")
        
    if 'deduplicate' not in cfg:
        cfg['deduplicate'] = False
    logger.info(f"Deduplication: {cfg['deduplicate']}")
    
    if 'owners' not in cfg:
        cfg['owners'] = False
    logger.info(f"Owner extraction: {cfg['owners']}")
    
    if 'output' not in cfg:
        cfg['output'] = {}
    if 'geopackage' not in cfg['output']:
        cfg['output']['geopackage'] = None
    if 'postgres' not in cfg['output']:
        cfg['output']['postgres'] = {}
    if 'dsn' not in cfg['output']['postgres']:
        cfg['output']['postgres']['dsn'] = None
    if 'features_cache' not in cfg:
        cfg['features_cache'] = 'new'
    if 'features_path' not in cfg:
        cfg['features_path'] = 'features.json'
    
    logger.info("YAML configuration loaded successfully")
    return cfg 
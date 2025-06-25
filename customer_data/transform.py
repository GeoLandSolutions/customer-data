import geopandas as gpd
import pandas as pd
import logging
from shapely.geometry import shape, Polygon, MultiPolygon, mapping

logger = logging.getLogger(__name__)

def esri_json_to_shapely(geom):
    """Convert ESRI JSON geometry to Shapely geometry"""
    if not geom or not isinstance(geom, dict):
        return None
    if 'rings' in geom:
        try:
            return Polygon(geom['rings'][0]) if len(geom['rings']) == 1 else MultiPolygon([Polygon(r) for r in geom['rings']])
        except Exception:
            return None
    if 'paths' in geom:
        # Polyline, treat as LineString
        from shapely.geometry import LineString, MultiLineString
        try:
            return LineString(geom['paths'][0]) if len(geom['paths']) == 1 else MultiLineString([LineString(p) for p in geom['paths']])
        except Exception:
            return None
    if 'x' in geom and 'y' in geom:
        from shapely.geometry import Point
        return Point(geom['x'], geom['y'])
    if 'type' in geom:
        try:
            return shape(geom)
        except Exception:
            return None
    return None

def features_to_gdf(meta, features):
    """Convert ArcGIS features to GeoDataFrame"""
    logger.info("Converting ArcGIS features to GeoDataFrame...")
    logger.info(f"Processing {len(features)} features")
    
    geoms = [esri_json_to_shapely(f.get('geometry')) for f in features]
    records = [f['attributes'] for f in features]
    
    # Count valid geometries
    valid_geoms = sum(1 for g in geoms if g is not None)
    logger.info(f"Valid geometries: {valid_geoms}/{len(geoms)}")
    
    gdf = gpd.GeoDataFrame(records, geometry=geoms, crs='EPSG:4326')
    logger.info(f"GeoDataFrame created with shape: {gdf.shape}")
    return gdf

def rest_data_to_gdf(meta, data):
    """Convert REST API data to GeoDataFrame or DataFrame"""
    logger.info("Converting REST API data...")
    logger.info(f"Processing {len(data)} records")
    
    if not data:
        logger.warning("No data to process")
        return pd.DataFrame()
    
    # Check if any records have geometry data
    has_geometry = False
    geom_fields_found = []
    
    for record in data:
        if isinstance(record, dict):
            # Look for common geometry field names
            geom_fields = ['geometry', 'geom', 'shape', 'coordinates', 'lat', 'lng', 'latitude', 'longitude']
            for field in geom_fields:
                if field in record and record[field] is not None:
                    has_geometry = True
                    if field not in geom_fields_found:
                        geom_fields_found.append(field)
                    break
            if has_geometry:
                break
    
    if has_geometry:
        logger.info(f"Geometry data detected in fields: {geom_fields_found}")
        logger.info("Converting to GeoDataFrame...")
        
        # Convert to GeoDataFrame
        geoms = []
        records = []
        
        for i, record in enumerate(data):
            geom = None
            # Try to extract geometry from various field names
            for field in ['geometry', 'geom', 'shape']:
                if field in record and record[field]:
                    try:
                        geom = shape(record[field])
                        break
                    except:
                        continue
            
            # Try coordinate fields
            if geom is None:
                lat = record.get('lat') or record.get('latitude')
                lng = record.get('lng') or record.get('longitude')
                if lat is not None and lng is not None:
                    from shapely.geometry import Point
                    try:
                        geom = Point(float(lng), float(lat))
                    except:
                        pass
            
            geoms.append(geom)
            records.append(record)
        
        valid_geoms = sum(1 for g in geoms if g is not None)
        logger.info(f"Valid geometries: {valid_geoms}/{len(geoms)}")
        
        gdf = gpd.GeoDataFrame(records, geometry=geoms, crs='EPSG:4326')
        logger.info(f"GeoDataFrame created with shape: {gdf.shape}")
        return gdf
    else:
        logger.info("No geometry data detected, converting to DataFrame...")
        # Convert to regular DataFrame
        df = pd.DataFrame(data)
        logger.info(f"DataFrame created with shape: {df.shape}")
        logger.info(f"Columns: {list(df.columns)}")
        return df

def data_to_gdf(meta, data):
    """Main function to convert data to GeoDataFrame or DataFrame based on API type"""
    api_type = meta.get('api_type', 'arcgis')
    logger.info(f"Converting data for API type: {api_type}")
    
    if api_type == 'arcgis':
        return features_to_gdf(meta, data)
    elif api_type == 'rest':
        return rest_data_to_gdf(meta, data)
    else:
        raise ValueError(f"Unsupported API type: {api_type}")

def deduplicate_gdf(gdf, primary_key):
    """Deduplicate GeoDataFrame or DataFrame by primary key"""
    logger.info(f"Deduplicating data using primary key: {primary_key}")
    
    # Check if primary key fields exist
    missing_keys = [key for key in primary_key if key not in gdf.columns]
    if missing_keys:
        logger.warning(f"Primary key fields not found: {missing_keys}")
        logger.warning(f"Available columns: {list(gdf.columns)}")
        return gdf
    
    original_count = len(gdf)
    gdf_dedup = gdf.drop_duplicates(subset=primary_key)
    final_count = len(gdf_dedup)
    
    logger.info(f"Deduplication: {original_count} -> {final_count} records")
    return gdf_dedup

def extract_owners(gdf):
    """Extract owner information from GeoDataFrame or DataFrame"""
    logger.info("Extracting owner information...")
    
    cols = [c for c in gdf.columns if 'owner' in c.lower()]
    if not cols:
        logger.warning("No owner-related columns found")
        return pd.DataFrame()
    
    logger.info(f"Found owner columns: {cols}")
    owners = gdf[cols].drop_duplicates().reset_index(drop=True)
    logger.info(f"Extracted {len(owners)} unique owner records")
    return owners 
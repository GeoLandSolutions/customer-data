import geopandas as gpd
import pandas as pd
from shapely.geometry import shape, Polygon, MultiPolygon, mapping

def esri_json_to_shapely(geom):
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
    geoms = [esri_json_to_shapely(f.get('geometry')) for f in features]
    records = [f['attributes'] for f in features]
    gdf = gpd.GeoDataFrame(records, geometry=geoms, crs='EPSG:4326')
    return gdf

def deduplicate_gdf(gdf, primary_key):
    return gdf.drop_duplicates(subset=primary_key)

def extract_owners(gdf):
    cols = [c for c in gdf.columns if 'owner' in c.lower()]
    if not cols:
        return pd.DataFrame()
    return gdf[cols].drop_duplicates().reset_index(drop=True) 
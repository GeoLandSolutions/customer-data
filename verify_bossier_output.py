import geopandas as gpd
import json

print('--- GeoPackage: features layer ---')
gdf = gpd.read_file('output/final/bossier.gpkg', layer='features')
print(gdf.info())
print(gdf.head())

print('\n--- GeoPackage: owners layer ---')
try:
    owners = gpd.read_file('output/final/bossier.gpkg', layer='owners')
    print(owners.info())
    print(owners.head())
except Exception as e:
    print('No owners layer or error:', e)

print('\n--- Features JSON ---')
with open('output/intermediate/bossier_features.json') as f:
    features = json.load(f)
print('Total features:', len(features))
print('First feature:', features[0] if features else None) 
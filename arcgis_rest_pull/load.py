import geopandas as gpd
import pandas as pd
import psycopg2

def write_geopackage(gdf, owners, path):
    gdf.to_file(path, layer='features', driver='GPKG')
    if owners is not None and not owners.empty:
        owners_gdf = gpd.GeoDataFrame(owners, geometry=None)
        owners_gdf.to_file(path, layer='owners', driver='GPKG')

def write_postgis(gdf, owners, dsn):
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    gdf_pg = gdf.copy()
    gdf_pg['geom'] = gdf_pg.geometry.apply(lambda x: x.wkt if x else None)
    cols = [c for c in gdf_pg.columns if c != 'geometry']
    cur.execute('DROP TABLE IF EXISTS features')
    cur.execute('CREATE TABLE features (' + ','.join(f'{c} text' for c in cols) + ', geom geometry)')
    for _, row in gdf_pg.iterrows():
        vals = [str(row[c]) for c in cols] + [row['geom']]
        cur.execute('INSERT INTO features VALUES (' + ','.join(['%s']*len(vals)) + ')', vals)
    if owners is not None and not owners.empty:
        cur.execute('DROP TABLE IF EXISTS owners')
        cur.execute('CREATE TABLE owners (' + ','.join(f'{c} text' for c in owners.columns) + ')')
        for _, row in owners.iterrows():
            vals = [str(row[c]) for c in owners.columns]
            cur.execute('INSERT INTO owners VALUES (' + ','.join(['%s']*len(vals)) + ')', vals)
    conn.commit()
    cur.close()
    conn.close() 
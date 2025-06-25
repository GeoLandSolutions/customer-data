import geopandas as gpd
import pandas as pd
import psycopg2
import os
import logging

logger = logging.getLogger(__name__)

def write_geopackage(data, owners, path):
    """Write data to GeoPackage, handling both GeoDataFrames and DataFrames"""
    logger.info(f"Writing GeoPackage to: {path}")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(path), exist_ok=True)
    logger.info(f"Ensured output directory exists: {os.path.dirname(path)}")
    
    if isinstance(data, gpd.GeoDataFrame):
        logger.info("Writing GeoDataFrame to GeoPackage...")
        data.to_file(path, layer='features', driver='GPKG')
        logger.info(f"Features layer written successfully")
        
        if owners is not None and not owners.empty:
            logger.info("Writing owners layer to GeoPackage...")
            owners_gdf = gpd.GeoDataFrame(owners, geometry=None)
            owners_gdf.to_file(path, layer='owners', driver='GPKG')
            logger.info(f"Owners layer written successfully")
    else:
        logger.info("Writing DataFrame to GeoPackage...")
        data_gdf = gpd.GeoDataFrame(data, geometry=None)
        data_gdf.to_file(path, layer='features', driver='GPKG')
        logger.info(f"Features layer written successfully")
        
        if owners is not None and not owners.empty:
            logger.info("Writing owners layer to GeoPackage...")
            owners_gdf = gpd.GeoDataFrame(owners, geometry=None)
            owners_gdf.to_file(path, layer='owners', driver='GPKG')
            logger.info(f"Owners layer written successfully")
    
    logger.info(f"GeoPackage written successfully: {path}")

def write_csv(data, owners, path):
    """Write data to CSV files"""
    logger.info(f"Writing CSV files...")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(path), exist_ok=True)
    logger.info(f"Ensured output directory exists: {os.path.dirname(path)}")
    
    # Write main data
    base_path = os.path.splitext(path)[0]
    main_csv_path = f"{base_path}.csv"
    
    logger.info(f"Writing main data to: {main_csv_path}")
    data.to_csv(main_csv_path, index=False)
    logger.info(f"Main CSV written successfully: {main_csv_path}")
    
    # Write owners if present
    if owners is not None and not owners.empty:
        owners_csv_path = f"{base_path}_owners.csv"
        logger.info(f"Writing owners data to: {owners_csv_path}")
        owners.to_csv(owners_csv_path, index=False)
        logger.info(f"Owners CSV written successfully: {owners_csv_path}")
    
    logger.info("CSV files written successfully")

def write_postgis(data, owners, dsn):
    """Write data to PostGIS, handling both GeoDataFrames and DataFrames"""
    logger.info("Writing to PostGIS...")
    
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        logger.info("Connected to PostgreSQL database")
        
        if isinstance(data, gpd.GeoDataFrame):
            logger.info("Writing GeoDataFrame to PostGIS...")
            data_pg = data.copy()
            data_pg['geom'] = data_pg.geometry.apply(lambda x: x.wkt if x else None)
            cols = [c for c in data_pg.columns if c != 'geometry']
            
            logger.info(f"Creating features table with columns: {cols}")
            cur.execute('DROP TABLE IF EXISTS features')
            cur.execute('CREATE TABLE features (' + ','.join(f'{c} text' for c in cols) + ', geom geometry)')
            
            logger.info(f"Inserting {len(data_pg)} records into features table...")
            for i, (_, row) in enumerate(data_pg.iterrows()):
                vals = [str(row[c]) for c in cols] + [row['geom']]
                cur.execute('INSERT INTO features VALUES (' + ','.join(['%s']*len(vals)) + ')', vals)
                
                if (i + 1) % 1000 == 0:
                    logger.info(f"Inserted {i + 1} records...")
            
            logger.info("Features table populated successfully")
        else:
            logger.info("Writing DataFrame to PostGIS...")
            cols = list(data.columns)
            
            logger.info(f"Creating features table with columns: {cols}")
            cur.execute('DROP TABLE IF EXISTS features')
            cur.execute('CREATE TABLE features (' + ','.join(f'{c} text' for c in cols) + ')')
            
            logger.info(f"Inserting {len(data)} records into features table...")
            for i, (_, row) in enumerate(data.iterrows()):
                vals = [str(row[c]) for c in cols]
                cur.execute('INSERT INTO features VALUES (' + ','.join(['%s']*len(vals)) + ')', vals)
                
                if (i + 1) % 1000 == 0:
                    logger.info(f"Inserted {i + 1} records...")
            
            logger.info("Features table populated successfully")
        
        # Write owners table
        if owners is not None and not owners.empty:
            logger.info("Writing owners table to PostGIS...")
            logger.info(f"Creating owners table with columns: {list(owners.columns)}")
            cur.execute('DROP TABLE IF EXISTS owners')
            cur.execute('CREATE TABLE owners (' + ','.join(f'{c} text' for c in owners.columns) + ')')
            
            logger.info(f"Inserting {len(owners)} records into owners table...")
            for i, (_, row) in enumerate(owners.iterrows()):
                vals = [str(row[c]) for c in owners.columns]
                cur.execute('INSERT INTO owners VALUES (' + ','.join(['%s']*len(vals)) + ')', vals)
                
                if (i + 1) % 1000 == 0:
                    logger.info(f"Inserted {i + 1} owner records...")
            
            logger.info("Owners table populated successfully")
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("PostGIS write completed successfully")
        
    except Exception as e:
        logger.error(f"Error writing to PostGIS: {e}")
        raise 
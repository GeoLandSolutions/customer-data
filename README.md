# customer-data

Extract data from various county assessor APIs (ArcGIS REST, custom REST APIs, etc.) to PostGIS and/or GeoPackage.

## Usage

### ArcGIS REST APIs (e.g., Bossier)
    python -m customer_data <config.yaml>

### Tulsa County API
    python -m customer_data <config.yaml> [data_type] [MM-DD-YYYY]

The Tulsa API supports three data types:
- `sales`: Valid sales data (default)
- `all`: All land lot parcel characteristics
- `values`: All actual and assessed values

Examples:
    # Extract sales data with default date
    python -m customer_data jurisdictions/tulsa.yaml
    
    # Extract sales data with specific date
    python -m customer_data jurisdictions/tulsa.yaml sales 01-01-2024
    
    # Extract all data with specific date
    python -m customer_data jurisdictions/tulsa.yaml all 06-10-2025
    
    # Extract all data with default date
    python -m customer_data jurisdictions/tulsa.yaml all
    
    # Extract all actual and assessed values with default date
    python -m customer_data jurisdictions/tulsa.yaml values
    
    # Extract all actual and assessed values with specific date
    python -m customer_data jurisdictions/tulsa.yaml values 06-10-2025

If no date is provided for Tulsa, it uses the date specified in the configuration file, which is set to 01-01-2025. 
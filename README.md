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

## Wayne, KY Integration

1. Create a `.env` file in the project root (if it doesn't exist):

```
WAYNE_KY_API_USER=your_actual_api_user
WAYNE_KY_API_KEY=your_actual_api_key
```

2. Edit `jurisdictions/wayne_ky.yaml` to ensure it contains:

```
name: Wayne, KY
api_base_url: "https://your-pvdnet-api-base-url.com/pvdnetapi/v1"
username_env: "WAYNE_KY_API_USER"
password_env: "WAYNE_KY_API_KEY"
api_type: wayne_ky
```

3. Run the ETL pipeline for Wayne, KY (example):

```
python -m customer_data --config jurisdictions/wayne_ky.yaml
```

Make sure you have the required dependencies installed (see `requirements.txt`). 
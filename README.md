# customer-data

Extract data from various county assessor APIs (ArcGIS REST, custom REST APIs, etc.) to PostGIS and/or GeoPackage.

## Usage

### ArcGIS REST APIs (e.g., Bossier)
    python -m customer_data <config.yaml>

### Tulsa County API
    python -m customer_data <config.yaml> [MM-DD-YYYY]

Examples:
    python -m customer_data jurisdictions/tulsa.yaml 01-01-2024
    python -m customer_data jurisdictions/tulsa.yaml 06-10-2025

If no date is provided for Tulsa, it uses the date specified in the configuration file, which is set to 01-01-2025. 
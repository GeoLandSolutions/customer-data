# customer-data

Extract data from ArcGIS FeatureServers and REST APIs to GeoPackage, CSV, and PostGIS.

## Usage

```bash
# ArcGIS FeatureServer
python -m customer_data jurisdictions/la/bossier/bossier.yaml

# REST API (Postman collection)
python -m customer_data jurisdictions/ok/tulsa/tulsa_assessor.json
```

## Configuration

### ArcGIS FeatureServer (YAML)

```yaml
url: "https://example.com/FeatureServer/0"
primary_key: ["OBJECTID"]
deduplicate: true
owners: true
output:
  geopackage: "output/la/bossier/final/bossier.gpkg"
features_cache: "new"
features_path: "output/la/bossier/intermediate/bossier_features.json"
```

### REST API (Postman Collection JSON)

Use Postman collections directly. The tool extracts endpoints, authentication, and variables automatically.

## Environment Variables

Create `.env` file for tokens:

```bash
TULSA_ASSESSOR_TOKEN=your_token_here
```

## Installation

```bash
pip install -r requirements.txt
```

## Examples

- `jurisdictions/la/bossier/bossier.yaml` - ArcGIS example
- `jurisdictions/ok/tulsa/tulsa_assessor.json` - REST API example 
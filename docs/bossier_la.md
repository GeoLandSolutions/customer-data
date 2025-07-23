# Bossier, LA (ArcGIS) ETL Guide

## Overview
Extracts parcel and assessment data from the Bossier Parish, LA ArcGIS REST API.

---

## Prerequisites
- No credentials required for public ArcGIS endpoints.
- Ensure you have internet access from your machine or VM.

---

## Configuration
Edit `jurisdictions/bossier.yaml`:
```yaml
name: Bossier, LA
url: "https://bpagis.bossierparish.org/server/rest/services/Parcels/BossierParcels_MarketAssessedLandUseValues_maplex_webmercator/FeatureServer/0"
api_type: bossier
primary_key: ["MappingNumber"]
deduplicate: true
owners: true
output:
  geopackage: output/final/bossier.gpkg
  postgres:
    dsn: null
features_cache: new
features_path: output/intermediate/bossier_features.json
```

---

## How to Run
```sh
python -m customer_data jurisdictions/bossier.yaml
```

---

## Output
- Metadata: `output/la/bossier/bossier_meta.json`
- Features: `output/la/bossier/bossier_features.json`

---

## Troubleshooting
- **No data:** The ArcGIS endpoint may be temporarily unavailable or empty.
- **No output:** Ensure the `output/` directory exists or let the script create it.
- **API errors:** Check the `url` in your config and your network connection.

---

## FAQ
**Q: Do I need an API key?**
A: No, public ArcGIS endpoints do not require authentication.

**Q: Where do I find my data?**
A: In the `output/la/bossier/` directory. 
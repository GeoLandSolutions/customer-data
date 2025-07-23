# Tulsa, OK ETL Guide

## Overview
Extracts property sales, parcel characteristics, and assessed values from the Tulsa County Assessor API.

---

## Prerequisites
- API token (if required by the endpoint).
- Add to `.env` in your project root if needed:
  ```
  TULSA_ASSESSOR_TOKEN=your_token
  ```
- Check your config for the correct API URLs and token usage.

---

## Configuration
Edit `jurisdictions/tulsa.yaml`:
```yaml
name: Tulsa, OK
api_type: tulsa
url: "https://api-assessor.tulsacounty.org/Modeling/GetAllValidSales"
url_all: "https://api-assessor.tulsacounty.org/Modeling/GetAllLandLotParcelCharacteristics"
url_values: "https://api-assessor.tulsacounty.org/Modeling/GetAllActualAndAssessedValues"
token: TULSA_ASSESSOR_TOKEN
lastModified: ''
output:
  json: output/tulsa.json
  csv: output/tulsa.csv
  geopackage: null
  postgres:
    dsn: null
primary_key: ["OBJECTID"]
deduplicate: false
owners: false
```
- `token` can be an environment variable name or a literal token.

---

## How to Run

You can specify the data type and date as command-line arguments:

```sh
python -m customer_data jurisdictions/tulsa.yaml [data_type] [MM-DD-YYYY]
```

### Supported Data Types
- `sales`: Valid sales data (**default**)
- `all`: All land lot parcel characteristics
- `values`: All actual and assessed values

### Examples
```sh
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
```

> **Note:** If no date is provided, the script uses the date specified in the configuration file, which is set to `01-01-2025` by default.

---

## Output
- Sales, parcel, or value data: `output/tulsa.json` and/or `output/tulsa.csv`
- Output location is set in your config file.

---

## Troubleshooting
- **Unauthorized:** Check your API token in `.env` or config.
- **No output:** Ensure the `output/` directory exists or let the script create it.
- **API errors:** Check the API URLs and your network connection.

---

## FAQ
**Q: Can I extract all parcel data?**
A: Yes, use the `all` data type as shown above.

**Q: Where do I find my data?**
A: In the `output/` directory, as specified in your config. 
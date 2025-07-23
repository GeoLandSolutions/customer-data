# Wayne, KY (PVDNet Adhoc API) ETL Guide

## Overview
Extracts parcel, owner, and assessment data from the Wayne, KY PVDNet Adhoc API.

---

## Prerequisites
- API username and key (contact Wayne County for access).
- Add to `.env` in your project root:
  ```
  WAYNE_KY_API_USER=your_user
  WAYNE_KY_API_KEY=your_key
  ```

---

## Configuration
Edit `jurisdictions/wayne_ky.yaml`:
```yaml
name: Wayne, KY
api_base_url: "https://kycowayne.pvdconnect.com/pvdnetapi/v1"
username_env: "WAYNE_KY_API_USER"
password_env: "WAYNE_KY_API_KEY"
api_type: wayne_ky
extract_all_tables: true
# Optionally, add adhoc_query: "SELECT TOP 10 * FROM <table>"
```
- `extract_all_tables: true` will fetch all available tables.

---

## How to Run
```sh
python -m customer_data jurisdictions/wayne_ky.yaml
```

---

## Output
- Adhoc tables list: `output/ky/wayne/wayne_ky_adhoc_tables.json`
- All table data: `output/ky/wayne/all_tables/wayne_ky_<table>.json`

---

## Troubleshooting
- **Unauthorized:** Check your API credentials in `.env`.
- **Empty files:** The source table may be empty.
- **No output:** Ensure the `output/` directory exists or let the script create it.

---

## FAQ
**Q: Can I run a custom query?**
A: Yes, add `adhoc_query: "SELECT ..."` to your config.

**Q: Where do I find my data?**
A: In the `output/ky/wayne/` directory. 
# Customer Data ETL Pipeline

Extract property and tax data from supported U.S. jurisdictions as JSON/CSV files.

## Install & Run
```sh
pip install -r requirements.txt
python -m customer_data jurisdictions/<your_jurisdiction>.yaml
```
- Edit configs in `jurisdictions/` as needed.
- Set up credentials in `.env` if required (see jurisdiction docs).

## Output
- Data is saved in `output/`, organized by jurisdiction.

## Jurisdiction Docs
- [Wayne, KY](docs/wayne_ky.md)
- [Bossier, LA](docs/bossier_la.md)
- [Tulsa, OK](docs/tulsa_ok.md)

See each document for config details, special options, and troubleshooting.

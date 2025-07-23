import os
import requests
import json
from dotenv import load_dotenv
from customer_data.etl.base import BaseJurisdictionETL
from customer_data.utils import ensure_dir_exists

class WayneKYETL(BaseJurisdictionETL):
    def extract(self, checkpoint_file=None):
        load_dotenv()
        cfg = self.cfg
        api_base_url = cfg['api_base_url']
        username = os.getenv(cfg['username_env'])
        password = os.getenv(cfg['password_env'])
        if not username or not password:
            raise ValueError(f"Missing credentials: username={username}, password={'set' if password else 'unset'}")
        auth_url = f"{api_base_url}/authenticate"
        payload = {"username": username, "password": password}
        headers = {"Content-Type": "application/json"}
        print(f"Authenticating to PVDNet API at {auth_url}...")
        r = requests.post(auth_url, json=payload, headers=headers)
        try:
            data = json.loads(r.content.decode("utf-8-sig"))
            token = data.get("token")
            resource_groups = data.get("resourceGroups")
        except Exception as e:
            print(f"Failed to decode JSON from response. Status: {r.status_code}")
            print(f"Response text: {r.text}")
            raise
        print("\n================ ACCESS TOKEN ================" )
        print(token)
        print("============================================\n")
        print("Resource Groups:")
        print(resource_groups)
        print("\nCopy this token and use it in the Swagger UI to explore endpoints.")
        # Set new output directory structure
        base_dir = os.path.join("output", "ky", "wayne")
        os.makedirs(base_dir, exist_ok=True)
        tables_output = os.path.join(base_dir, "wayne_ky_adhoc_tables.json")
        ensure_dir_exists(tables_output)
        self.fetch_adhoc_tables(api_base_url, token, tables_output)
        # Run a sample Adhoc query if a query is provided in config
        adhoc_query = cfg.get('adhoc_query')
        if adhoc_query:
            query_output = os.path.join(base_dir, "wayne_ky_adhoc_query.json")
            ensure_dir_exists(query_output)
            self.run_adhoc_query(api_base_url, token, adhoc_query, query_output)
        # Extract all tables if requested
        if cfg.get('extract_all_tables'):
            output_dir = os.path.join(base_dir, "all_tables")
            os.makedirs(output_dir, exist_ok=True)
            self.extract_all_adhoc_tables(api_base_url, token, tables_output, output_dir)
        return {"token": token, "resourceGroups": resource_groups}

    def transform(self, data):
        # No-op for now
        return data

    def load(self, data):
        # No-op for now
        return data

    def fetch_adhoc_tables(self, api_base_url, token, output_path):
        endpoint = "/adhoc/tables"
        headers = {"AccessToken": token}
        url = f"{api_base_url}{endpoint}"
        print(f"Fetching Adhoc tables from {url}")
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        tables = json.loads(resp.content.decode("utf-8-sig"))
        ensure_dir_exists(output_path)
        with open(output_path, "w") as f:
            json.dump(tables, f, indent=2)
        print(f"Saved Adhoc tables to {output_path}")
        return tables

    def run_adhoc_query(self, api_base_url, token, query, output_path):
        endpoint = "/adhoc/tables/query"
        headers = {"AccessToken": token, "Content-Type": "application/json"}
        url = f"{api_base_url}{endpoint}"
        payload = {"Query": query}
        print(f"Running Adhoc query: {query}")
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        results = json.loads(resp.content.decode("utf-8-sig"))
        ensure_dir_exists(output_path)
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Saved Adhoc query results to {output_path}")
        return results

    def extract_all_adhoc_tables(self, api_base_url, token, tables_json_path, output_dir):
        print(f"Loading table list from {tables_json_path}")
        print(f"Current working directory: {os.getcwd()}")
        with open(tables_json_path) as f:
            tables_info = json.load(f)
        tables = tables_info.get("tables", [])
        os.makedirs(output_dir, exist_ok=True)
        for table in tables:
            table_name = table["name"]
            print(f"Extracting all data from table: {table_name}")
            query = f"SELECT * FROM {table_name}"
            output_path = os.path.join(output_dir, f"wayne_ky_{table_name}.json")
            print(f"Writing to: {output_path}")
            try:
                ensure_dir_exists(output_path)
                self.run_adhoc_query(api_base_url, token, query, output_path)
                print(f"File exists after write? {os.path.exists(output_path)}")
            except Exception as e:
                print(f"Failed to extract table {table_name}: {e}") 
import os
from dotenv import load_dotenv
import json
import requests

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


def write_to_dbfs(data, path):
    content = json.dumps(data).encode("utf-8")

    response = requests.put(
        f"{DATABRICKS_HOST}/api/2.0/fs/files{path}?overwrite=true",
        headers={
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/octet-stream",
        },
        data=content,
    )
    return response.status_code


file_names = ["users", "products", "orders_history"]

for name in file_names:
    with open(f"data/{name}.json", "r") as f:
        data = json.load(f)

    path = f"/Volumes/ecommerce/bronze/batch_files/{name}/{name}.json"
    status = write_to_dbfs(data, path)
    print(f"Uploaded {path} - status {status}")

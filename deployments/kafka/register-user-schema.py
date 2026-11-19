#!/usr/bin/env python3
"""Register the example user schema, requiring it to be subject version 1."""
import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

subject = "example_user_topic-value"
base = os.environ.get("SCHEMA_REGISTRY_HOST_URL", "http://localhost:18081").rstrip("/")
schema = json.loads((Path(__file__).parent / "schemas" / f"{subject}.json").read_text())
url = f"{base}/subjects/{subject}/versions"

try:
    with urlopen(f"{url}/1", timeout=10) as response:
        existing = json.load(response)
except HTTPError as error:
    if error.code != 404:
        raise
else:
    if existing.get("schemaType", "AVRO") != "JSON" or json.loads(existing["schema"]) != schema:
        raise SystemExit(f"{subject} version 1 already exists with a different schema")
    print(f"{subject} version 1 already registered (id {existing['id']})")
    raise SystemExit(0)

payload = json.dumps({"schemaType": "JSON", "schema": json.dumps(schema)}).encode()
request = Request(url, data=payload, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
with urlopen(request, timeout=10) as response:
    result = json.load(response)
with urlopen(f"{url}/1", timeout=10) as response:
    registered = json.load(response)
if registered["id"] != result["id"] or json.loads(registered["schema"]) != schema:
    raise SystemExit(f"{subject} was registered but is not version 1; inspect the registry")
print(f"Registered {subject} version 1 (id {result['id']})")

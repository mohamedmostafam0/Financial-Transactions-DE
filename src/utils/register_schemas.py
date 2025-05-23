"""
Schema registration utility for Avro schemas.
"""
import json
import requests
import sys
from src.utils.config import SCHEMA_REGISTRY

def register_schema(schema_file, subject, registry_url=None):
    """Register a schema with the Schema Registry.
    
    Args:
        schema_file: Path to the schema file
        subject: Subject name in the registry
        registry_url: URL of the schema registry (optional, defaults to config)
        
    Returns:
        True if registration successful, False otherwise
    """
    if registry_url is None:
        registry_url = SCHEMA_REGISTRY["URL"]
        
    with open(schema_file, 'r') as f:
        schema = json.load(f)
    
    schema_json = json.dumps(schema)
    headers = {'Content-Type': 'application/vnd.schemaregistry.v1+json'}
    payload = {"schema": schema_json}
    
    r = requests.post(
        f"{registry_url}/subjects/{subject}/versions",
        data=json.dumps(payload),
        headers=headers
    )
    
    if r.status_code in (200, 201):
        result = r.json()
        version = result.get('id')
        print(f"✅ Schema registered successfully. ID: {version}")
        return True
    else:
        print(f"❌ Error registering schema: {r.text}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python register_schemas.py <schema_file> <subject>")
        sys.exit(1)
    
    schema_file = sys.argv[1]
    subject = sys.argv[2]
    
    if not register_schema(schema_file, subject):
        sys.exit(1)

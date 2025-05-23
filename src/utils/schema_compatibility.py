"""
Schema compatibility checker for Avro schemas.
"""
import json
import requests
import sys
from src.utils.config import SCHEMA_REGISTRY

def check_schema_compatibility(schema_file, subject, registry_url=None):
    """Check if a schema is compatible with the latest version in the registry.
    
    Args:
        schema_file: Path to the schema file
        subject: Subject name in the registry
        registry_url: URL of the schema registry (optional, defaults to config)
        
    Returns:
        True if compatible, False otherwise
    """
    if registry_url is None:
        registry_url = SCHEMA_REGISTRY["URL"]
        
    with open(schema_file, 'r') as f:
        schema = json.load(f)
    
    schema_json = json.dumps(schema)
    headers = {'Content-Type': 'application/vnd.schemaregistry.v1+json'}
    
    # Check if subject exists
    r = requests.get(f"{registry_url}/subjects/{subject}/versions/latest", headers=headers)
    if r.status_code == 404:
        print(f"Subject {subject} not found. This will be the first version.")
        return True
    
    # Check compatibility
    payload = {"schema": schema_json}
    r = requests.post(
        f"{registry_url}/compatibility/subjects/{subject}/versions/latest",
        data=json.dumps(payload),
        headers=headers
    )
    
    if r.status_code == 200:
        result = r.json()
        is_compatible = result.get('is_compatible', False)
        if is_compatible:
            print(f"✅ Schema is compatible with {subject}")
            return True
        else:
            print(f"❌ Schema is NOT compatible with {subject}")
            return False
    else:
        print(f"❌ Error checking compatibility: {r.text}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python schema_compatibility.py <schema_file> <subject>")
        sys.exit(1)
    
    schema_file = sys.argv[1]
    subject = sys.argv[2]
    
    if not check_schema_compatibility(schema_file, subject):
        sys.exit(1)

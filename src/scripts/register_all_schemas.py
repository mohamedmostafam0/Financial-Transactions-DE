"""
Script to register all Avro schemas with the Schema Registry.
"""
import os
import subprocess
import sys
from src.utils.config import SCHEMA_REGISTRY
from src.utils.schema_compatibility import check_schema_compatibility
from src.utils.register_schemas import register_schema

def register_all_schemas():
    """Register all schemas with the Schema Registry."""
    schema_dir = os.path.join('src', 'schemas', 'avro')
    registry_url = SCHEMA_REGISTRY["URL"]
    
    for schema_file in os.listdir(schema_dir):
        if schema_file.endswith('.avsc'):
            schema_name = schema_file.split('.')[0]
            subject = f"{schema_name}-value"
            schema_path = os.path.join(schema_dir, schema_file)
            
            print(f"Processing schema: {schema_name}")
            
            # Check compatibility first
            if check_schema_compatibility(schema_path, subject, registry_url):
                # Register schema
                if register_schema(schema_path, subject, registry_url):
                    print(f"✅ Registered {schema_name} successfully")
                else:
                    print(f"❌ Registration failed for {schema_name}")
            else:
                print(f"❌ Compatibility check failed for {schema_name}")

if __name__ == "__main__":
    register_all_schemas()

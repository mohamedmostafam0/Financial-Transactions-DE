"""
Schema Registry utility for Avro serialization/deserialization.
"""
import json
import os
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer

class SchemaRegistry:
    """Utility class for managing schema registry operations."""
    
    def __init__(self, schema_registry_url):
        """Initialize the schema registry client.
        
        Args:
            schema_registry_url: URL of the schema registry
        """
        self.client = SchemaRegistryClient({'url': schema_registry_url})
        self.serializers = {}
        self.deserializers = {}
        self.schemas = {}
        
    def load_schema(self, schema_path):
        """Load an Avro schema from a file.
        
        Args:
            schema_path: Path to the schema file
            
        Returns:
            The loaded schema as a dictionary
        """
        with open(schema_path, 'r') as f:
            return json.load(f)
            
    def get_schema(self, schema_name):
        """Get a schema by name, loading it if necessary.
        
        Args:
            schema_name: Name of the schema (without extension)
            
        Returns:
            The schema as a dictionary
        """
        if schema_name not in self.schemas:
            schema_path = os.path.join('src', 'schemas', 'avro', f"{schema_name}.avsc")
            self.schemas[schema_name] = self.load_schema(schema_path)
        return self.schemas[schema_name]
        
    def get_serializer(self, schema_name):
        """Get an Avro serializer for a schema.
        
        Args:
            schema_name: Name of the schema (without extension)
            
        Returns:
            An AvroSerializer instance
        """
        if schema_name not in self.serializers:
            schema = self.get_schema(schema_name)
            self.serializers[schema_name] = AvroSerializer(
                self.client,
                json.dumps(schema),
                lambda obj, ctx: obj
            )
        return self.serializers[schema_name]
        
    def get_deserializer(self, schema_name):
        """Get an Avro deserializer for a schema.
        
        Args:
            schema_name: Name of the schema (without extension)
            
        Returns:
            An AvroDeserializer instance
        """
        if schema_name not in self.deserializers:
            schema = self.get_schema(schema_name)
            self.deserializers[schema_name] = AvroDeserializer(
                self.client,
                json.dumps(schema)
            )
        return self.deserializers[schema_name]
        
    def get_key_serializer(self):
        """Get a string serializer for keys.
        
        Returns:
            A StringSerializer instance
        """
        return StringSerializer()
        
    def get_key_deserializer(self):
        """Get a string deserializer for keys.
        
        Returns:
            A StringDeserializer instance
        """
        return StringDeserializer()

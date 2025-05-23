"""
Redis cache utility for Financial Transactions pipeline.
Provides caching layer between producers and consumers.
"""
import json
import logging
import os
import redis
from typing import Dict, Any, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
REDIS_EXPIRY = int(os.getenv('REDIS_EXPIRY', 3600))  # Default 1 hour


class RedisCache:
    """Redis cache utility for storing and retrieving data."""
    
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, 
                 password=REDIS_PASSWORD, expiry=REDIS_EXPIRY):
        """Initialize Redis connection.
        
        Args:
            host (str): Redis host
            port (int): Redis port
            db (int): Redis database number
            password (str): Redis password
            expiry (int): Default expiry time in seconds
        """
        self.expiry = expiry
        try:
            self.redis = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            logger.info(f"Connected to Redis at {host}:{port}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis = None
    
    def is_connected(self) -> bool:
        """Check if Redis connection is active.
        
        Returns:
            bool: True if connected, False otherwise
        """
        if not self.redis:
            return False
        try:
            return self.redis.ping()
        except (redis.ConnectionError, redis.TimeoutError):
            return False
    
    def set(self, key: str, value: Union[str, Dict[str, Any]], 
            expiry: Optional[int] = None) -> bool:
        """Set a value in Redis cache.
        
        Args:
            key (str): Cache key
            value (Union[str, Dict]): Value to cache (will be JSON serialized if dict)
            expiry (int, optional): Custom expiry time in seconds
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.is_connected():
            logger.warning("Redis not connected, skipping cache set")
            return False
        
        try:
            if isinstance(value, dict):
                value = json.dumps(value)
            
            exp = expiry if expiry is not None else self.expiry
            return self.redis.set(key, value, ex=exp)
        except Exception as e:
            logger.error(f"Error setting cache key {key}: {e}")
            return False
    
    def get(self, key: str, as_json: bool = True) -> Optional[Union[str, Dict[str, Any]]]:
        """Get a value from Redis cache.
        
        Args:
            key (str): Cache key
            as_json (bool): Whether to parse result as JSON
            
        Returns:
            Optional[Union[str, Dict]]: Cached value or None if not found
        """
        if not self.is_connected():
            logger.warning("Redis not connected, skipping cache get")
            return None
        
        try:
            value = self.redis.get(key)
            if value and as_json:
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return value
        except Exception as e:
            logger.error(f"Error getting cache key {key}: {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """Delete a key from Redis cache.
        
        Args:
            key (str): Cache key
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.is_connected():
            logger.warning("Redis not connected, skipping cache delete")
            return False
        
        try:
            return bool(self.redis.delete(key))
        except Exception as e:
            logger.error(f"Error deleting cache key {key}: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in Redis cache.
        
        Args:
            key (str): Cache key
            
        Returns:
            bool: True if key exists, False otherwise
        """
        if not self.is_connected():
            logger.warning("Redis not connected, skipping cache exists check")
            return False
        
        try:
            return bool(self.redis.exists(key))
        except Exception as e:
            logger.error(f"Error checking if key {key} exists: {e}")
            return False
    
    def set_hash(self, key: str, mapping: Dict[str, Any], 
                expiry: Optional[int] = None) -> bool:
        """Set a hash in Redis cache.
        
        Args:
            key (str): Cache key
            mapping (Dict): Hash mapping to store
            expiry (int, optional): Custom expiry time in seconds
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.is_connected():
            logger.warning("Redis not connected, skipping hash set")
            return False
        
        try:
            # Convert any dict values to JSON strings
            serialized_mapping = {}
            for k, v in mapping.items():
                if isinstance(v, dict):
                    serialized_mapping[k] = json.dumps(v)
                else:
                    serialized_mapping[k] = v
            
            result = self.redis.hset(key, mapping=serialized_mapping)
            
            # Set expiry if provided
            if expiry is not None or self.expiry:
                exp = expiry if expiry is not None else self.expiry
                self.redis.expire(key, exp)
                
            return result > 0
        except Exception as e:
            logger.error(f"Error setting hash for key {key}: {e}")
            return False
    
    def get_hash(self, key: str, field: Optional[str] = None) -> Union[Dict[str, Any], Any, None]:
        """Get a hash or hash field from Redis cache.
        
        Args:
            key (str): Cache key
            field (str, optional): Hash field to get
            
        Returns:
            Union[Dict, Any, None]: Hash mapping, field value, or None if not found
        """
        if not self.is_connected():
            logger.warning("Redis not connected, skipping hash get")
            return None
        
        try:
            if field:
                value = self.redis.hget(key, field)
                # Try to parse as JSON if it looks like a JSON string
                if value and value.startswith('{') and value.endswith('}'):
                    try:
                        return json.loads(value)
                    except json.JSONDecodeError:
                        return value
                return value
            else:
                result = self.redis.hgetall(key)
                # Try to parse any JSON string values
                for k, v in result.items():
                    if isinstance(v, str) and v.startswith('{') and v.endswith('}'):
                        try:
                            result[k] = json.loads(v)
                        except json.JSONDecodeError:
                            pass
                return result or None
        except Exception as e:
            logger.error(f"Error getting hash for key {key}: {e}")
            return None
    
    def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """Increment a counter in Redis.
        
        Args:
            key (str): Counter key
            amount (int): Amount to increment by
            
        Returns:
            Optional[int]: New value or None if failed
        """
        if not self.is_connected():
            logger.warning("Redis not connected, skipping increment")
            return None
        
        try:
            return self.redis.incrby(key, amount)
        except Exception as e:
            logger.error(f"Error incrementing key {key}: {e}")
            return None

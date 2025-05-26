"""
PostgreSQL database utility for Financial Transactions pipeline.
Provides database access layer between producers and consumers.
"""
import json
import logging
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_values
from typing import Dict, Any, Optional, Union, List, Tuple
from src.utils.config import POSTGRES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# PostgreSQL configuration from config.py
PG_HOST = POSTGRES["HOST"]
PG_PORT = int(POSTGRES["PORT"])
PG_DATABASE = POSTGRES["DATABASE"]
PG_USER = POSTGRES["USER"]
PG_PASSWORD = POSTGRES["PASSWORD"]
PG_MIN_CONN = 1  # Minimum connections in pool
PG_MAX_CONN = 10  # Maximum connections in pool


class PostgresDB:
    """PostgreSQL database utility for storing and retrieving data."""
    
    _instance = None
    _connection_pool = None
    
    def __new__(cls, *args, **kwargs):
        """Implement singleton pattern to ensure one connection pool."""
        if cls._instance is None:
            cls._instance = super(PostgresDB, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, 
                 user=PG_USER, password=PG_PASSWORD, 
                 min_conn=PG_MIN_CONN, max_conn=PG_MAX_CONN):
        """Initialize PostgreSQL connection pool.
        
        Args:
            host (str): PostgreSQL host
            port (int): PostgreSQL port
            dbname (str): Database name
            user (str): Database user
            password (str): Database password
            min_conn (int): Minimum connections in pool
            max_conn (int): Maximum connections in pool
        """
        if self._initialized:
            return
            
        self.db_params = {
            'host': host,
            'port': port,
            'dbname': dbname,
            'user': user,
            'password': password
        }
        
        try:
            self._connection_pool = psycopg2.pool.ThreadedConnectionPool(
                min_conn, max_conn, **self.db_params
            )
            logger.info(f"Connected to PostgreSQL at {host}:{port}")
            
            # Initialize database schema
            self._init_schema()
            
            self._initialized = True
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            self._connection_pool = None
    
    def _init_schema(self):
        """Initialize database schema if it doesn't exist."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Create users table with JSONB for nested data
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    age INTEGER,
                    gender VARCHAR(10),
                    occupation VARCHAR(255),
                    account_created TIMESTAMP WITH TIME ZONE,
                    location JSONB,
                    financial JSONB,
                    preferences JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # Create merchants table with JSONB for nested data
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS merchants (
                    merchant_id VARCHAR(36) PRIMARY KEY,
                    merchant_name VARCHAR(255) NOT NULL,
                    merchant_category VARCHAR(100) NOT NULL,
                    location JSONB,
                    business JSONB,
                    payment JSONB,
                    risk JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # Create blacklist table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS blacklist (
                    id SERIAL PRIMARY KEY,
                    entity_type VARCHAR(50) NOT NULL,
                    entity_id VARCHAR(36) NOT NULL,
                    reason TEXT NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(entity_type, entity_id)
                )
                """)
                
                # Create indexes
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_blacklist_entity ON blacklist(entity_type, entity_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_blacklist_timestamp ON blacklist(timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_location ON users USING GIN (location)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_merchants_location ON merchants USING GIN (location)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_merchants_category ON merchants(merchant_category)")
                
                conn.commit()
                logger.info("Database schema initialized")
    
    def is_connected(self) -> bool:
        """Check if PostgreSQL connection is active.
        
        Returns:
            bool: True if connected, False otherwise
        """
        if not self._connection_pool:
            return False
        
        conn = None
        try:
            conn = self._connection_pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except psycopg2.Error:
            return False
        finally:
            if conn:
                self._connection_pool.putconn(conn)
    
    def get_connection(self):
        """Get a connection from the pool.
        
        Returns:
            Connection: PostgreSQL connection
        
        Raises:
            Exception: If connection pool is not initialized
        """
        if not self._connection_pool:
            raise Exception("Database connection pool not initialized")
        
        return self._connection_pool.getconn()
    
    def release_connection(self, conn):
        """Return a connection to the pool.
        
        Args:
            conn: PostgreSQL connection
        """
        if self._connection_pool:
            self._connection_pool.putconn(conn)
    
    def insert_user(self, user: Dict[str, Any]) -> bool:
        """Insert or update a user in the database.
        
        Args:
            user (Dict): User data
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.is_connected():
            logger.warning("PostgreSQL not connected, skipping user insert")
            return False
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Extract basic fields
                user_id = user['user_id']
                name = user['name']
                email = user['email']
                
                # Extract optional fields with defaults
                age = user.get('age')
                gender = user.get('gender')
                occupation = user.get('occupation')
                account_created = user.get('account_created')
                
                # Extract JSON fields
                location = json.dumps(user.get('location', {})) if user.get('location') else None
                financial = json.dumps(user.get('financial', {})) if user.get('financial') else None
                preferences = json.dumps(user.get('preferences', {})) if user.get('preferences') else None
                
                cursor.execute("""
                INSERT INTO users (
                    user_id, name, email, age, gender, occupation, 
                    account_created, location, financial, preferences, 
                    created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) 
                DO UPDATE SET 
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    age = EXCLUDED.age,
                    gender = EXCLUDED.gender,
                    occupation = EXCLUDED.occupation,
                    account_created = EXCLUDED.account_created,
                    location = EXCLUDED.location,
                    financial = EXCLUDED.financial,
                    preferences = EXCLUDED.preferences,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING user_id
                """, (
                    user_id, name, email, age, gender, occupation, 
                    account_created, location, financial, preferences
                ))
                
                result = cursor.fetchone()
                conn.commit()
                return result is not None
        except psycopg2.Error as e:
            logger.error(f"Error inserting user: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.release_connection(conn)
    
    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get a user from the database.
        
        Args:
            user_id (int): User ID
            
        Returns:
            Optional[Dict]: User data or None if not found
        """
        if not self.is_connected():
            logger.warning("PostgreSQL not connected, skipping user get")
            return None
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                SELECT user_id, name, email
                FROM users
                WHERE user_id = %s
                """, (user_id,))
                
                result = cursor.fetchone()
                return dict(result) if result else None
        except psycopg2.Error as e:
            logger.error(f"Error getting user: {e}")
            return None
        finally:
            if conn:
                self.release_connection(conn)
    
    def get_random_user(self) -> Optional[Dict[str, Any]]:
        """Get a random user from the database.
        
        Returns:
            Optional[Dict]: User data or None if no users exist
        """
        if not self.is_connected():
            logger.warning("PostgreSQL not connected, skipping random user get")
            return None
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                SELECT user_id, name, email
                FROM users
                ORDER BY RANDOM()
                LIMIT 1
                """)
                
                result = cursor.fetchone()
                return dict(result) if result else None
        except psycopg2.Error as e:
            logger.error(f"Error getting random user: {e}")
            return None
        finally:
            if conn:
                self.release_connection(conn)
    
    def insert_merchant(self, merchant: Dict[str, Any]) -> bool:
        """Insert or update a merchant in the database.
        
        Args:
            merchant (Dict): Merchant data
            
        Returns:
        finally:
            if conn:
                self.release_connection(conn)
    
    def get_merchant(self, merchant_id: str) -> Optional[Dict[str, Any]]:
        """Get a merchant from the database.
        
        Args:
            merchant_id (str): Merchant ID
            
        Returns:
            Optional[Dict]: Merchant data or None if not found
        """
        if not self.is_connected():
            logger.warning("PostgreSQL not connected, skipping merchant get")
            return None
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                SELECT merchant_id, merchant_name, merchant_category
                FROM merchants
                WHERE merchant_id = %s
                """, (merchant_id,))
                
                result = cursor.fetchone()
                return dict(result) if result else None
        except psycopg2.Error as e:
            logger.error(f"Error getting merchant: {e}")
            return None
        finally:
            if conn:
                self.release_connection(conn)
    
    def get_random_merchant(self) -> Optional[Dict[str, Any]]:
        """Get a random merchant from the database.
        
        Returns:
            Optional[Dict]: Merchant data or None if no merchants exist
        """
        if not self.is_connected():
            logger.warning("PostgreSQL not connected, skipping random merchant get")
            return None
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                SELECT merchant_id, merchant_name, merchant_category
                FROM merchants
                ORDER BY RANDOM()
                LIMIT 1
                """)
                
                result = cursor.fetchone()
                return dict(result) if result else None
        except psycopg2.Error as e:
            logger.error(f"Error getting random merchant: {e}")
            return None
        finally:
            if conn:
                self.release_connection(conn)
    
    def get_users_batch(self, limit=100, offset=0):
        """Get a batch of users from the database.
        
        Args:
            limit (int): Maximum number of users to return
            offset (int): Offset for pagination
            
        Returns:
            List[Dict]: List of user data or empty list if no users exist
        """
        if not self.is_connected():
            logger.warning("PostgreSQL not connected, skipping users batch get")
            return []
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                SELECT user_id, name, email
                FROM users
                ORDER BY user_id
                LIMIT %s OFFSET %s
                """, (limit, offset))
                
                results = cursor.fetchall()
                return [dict(row) for row in results] if results else []
        except psycopg2.Error as e:
            logger.error(f"Error getting users batch: {e}")
            return []
        finally:
            if conn:
                self.release_connection(conn)
    
    def get_merchants_batch(self, limit=100, offset=0):
        """Get a batch of merchants from the database.
        
        Args:
            limit (int): Maximum number of merchants to return
            offset (int): Offset for pagination
            
        Returns:
            List[Dict]: List of merchant data or empty list if no merchants exist
        """
        if not self.is_connected():
            logger.warning("PostgreSQL not connected, skipping merchants batch get")
            return []
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                SELECT merchant_id, merchant_name, merchant_category
                FROM merchants
                ORDER BY merchant_id
                LIMIT %s OFFSET %s
                """, (limit, offset))
                
                results = cursor.fetchall()
                return [dict(row) for row in results] if results else []
        except psycopg2.Error as e:
            logger.error(f"Error getting merchants batch: {e}")
            return []
        finally:
            if conn:
                self.release_connection(conn)
    
    def insert_blacklist(self, blacklist: Dict[str, Any]) -> bool:
        """Insert or update a blacklist entry in the database.
        
        Args:
            blacklist (Dict): Blacklist data
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.is_connected():
            logger.warning("PostgreSQL not connected, skipping blacklist insert")
            return False
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                INSERT INTO blacklist (entity_type, entity_id, reason, timestamp)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (entity_type, entity_id) 
                DO UPDATE SET 
                    reason = EXCLUDED.reason,
                    timestamp = EXCLUDED.timestamp,
                    created_at = CURRENT_TIMESTAMP
                RETURNING id
                """, (
                    blacklist['entity_type'],
                    blacklist['entity_id'],
                    blacklist['reason'],
                    blacklist['timestamp']
                ))
                
                result = cursor.fetchone()
                conn.commit()
                return result is not None
        except psycopg2.Error as e:
            logger.error(f"Error inserting blacklist: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.release_connection(conn)
    
    def is_blacklisted(self, entity_type: str, entity_id: str) -> bool:
        """Check if an entity is blacklisted.
        
        Args:
            entity_type (str): Entity type (user, merchant)
            entity_id (str): Entity ID
            
        Returns:
            bool: True if blacklisted, False otherwise
        """
        if not self.is_connected():
            logger.warning("PostgreSQL not connected, skipping blacklist check")
            return False
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT 1 FROM blacklist
                WHERE entity_type = %s AND entity_id = %s
                LIMIT 1
                """, (entity_type, entity_id))
                
                result = cursor.fetchone()
                return result is not None
        except psycopg2.Error as e:
            logger.error(f"Error checking blacklist: {e}")
            return False
        finally:
            if conn:
                self.release_connection(conn)
    
    def close(self):
        """Close all database connections."""
        if self._connection_pool:
            self._connection_pool.closeall()
            logger.info("Closed all database connections")
            self._connection_pool = None

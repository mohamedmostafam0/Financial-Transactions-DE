# src/producers/user_producer.py
import sys
import time
import logging
from faker import Faker
from src.utils.postgres_db import PostgresDB
from src.utils.config import POSTGRES, KAFKA

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()

# Get configuration from config.py
PG_ENABLED = POSTGRES["ENABLED"]

# Sleep interval between user generation (seconds)
PRODUCER_INTERVAL = KAFKA["PRODUCER_INTERVAL"]

# Initialize PostgreSQL connection
db = None
if PG_ENABLED:
    db = PostgresDB()
    if db.is_connected():
        logger.info("PostgreSQL database initialized and connected")
    else:
        logger.error("PostgreSQL database connection failed")
        sys.exit(1)  # Exit if database connection fails

def generate_user():
    """Generate a user with random data"""
    return {
        "user_id": fake.random_int(min=1, max=10000),
        "name": fake.name(),
        "email": fake.email()
    }

def run():
    """Main function to generate users and store them in PostgreSQL"""
    logger.info("🚀 User producer started - writing to PostgreSQL only")
    
    # Track users we've already generated
    user_count = 0
    
    try:
        while True:
            user = generate_user()
            user_id = user["user_id"]
            
            # Store in PostgreSQL
            if db.insert_user(user):
                logger.info(f"✅ Stored user in database: {user_id}")
                user_count += 1
                
                # Every 100 users, log a summary
                if user_count % 100 == 0:
                    logger.info(f"Generated {user_count} users so far")
            else:
                logger.warning(f"❌ Failed to store user in database: {user_id}")
            
            # Sleep before generating next user
            time.sleep(PRODUCER_INTERVAL)
    except KeyboardInterrupt:
        logger.info("User producer stopped by user")
    except Exception as e:
        logger.error(f"Error in user producer: {e}", exc_info=True)
    finally:
        logger.info(f"User producer finished after generating {user_count} users")

if __name__ == "__main__":
    run()

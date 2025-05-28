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
    # Generate a random date in the past 5 years for account creation
    account_created = fake.date_time_between(start_date='-5y', end_date='now')
    
    # Generate location data
    country_code = fake.country_code()
    city = fake.city()
    latitude = float(fake.latitude())
    longitude = float(fake.longitude())
    
    # Generate financial attributes
    credit_score = random.randint(300, 850)
    risk_score = random.randint(1, 100)
    
    # Generate user preferences
    preferred_payment_methods = random.sample(
        ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Mobile Payment"], 
        k=random.randint(1, 3)
    )
    
    # Generate spending categories preferences (weighted)
    spending_categories = {}
    categories = ["Retail", "Electronics", "Travel", "Dining", "Services", 
                 "Health", "Entertainment", "Education", "Finance"]
    
    for category in categories:
        # Assign a random weight to each category (1-10)
        spending_categories[category] = random.randint(1, 10)
    
    return {
        "user_id": fake.random_int(min=1, max=10000),
        "name": fake.name(),
        "email": fake.email(),
        "age": random.randint(18, 85),
        "gender": random.choice(["M", "F"]),
        "occupation": fake.job(),
        "account_created": account_created.isoformat(),
        "location": {
            "country": country_code,
            "city": city,
            "address": fake.street_address(),
            "postal_code": fake.postcode(),
            "latitude": latitude,
            "longitude": longitude
        },
        "financial": {
            "credit_score": credit_score,
            "risk_score": risk_score,
            "income_bracket": random.choice(["Low", "Medium", "High", "Very High"]),
            "preferred_payment_methods": preferred_payment_methods
        },
        "preferences": {
            "spending_categories": spending_categories,
            "communication_channel": random.choice(["Email", "SMS", "Push Notification", "None"])
        }
    }

def run():
    """Main function to generate users and store them in PostgreSQL"""
    logger.info("User producer started - writing to PostgreSQL only")
    
    # Track users we've already generated
    user_count = 0
    
    try:
        while True:
            user = generate_user()
            user_id = user["user_id"]
            
            # Store in PostgreSQL
            if db.insert_user(user):
                logger.info(f"Stored user in database: {user_id}")
                user_count += 1
                
                # Every 100 users, log a summary
                if user_count % 100 == 0:
                    logger.info(f"Generated {user_count} users so far")
            else:
                logger.warning(f"Failed to store user in database: {user_id}")
            
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

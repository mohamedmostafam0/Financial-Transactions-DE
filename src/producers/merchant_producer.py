# src/producers/merchant_producer.py
import sys
import time
import uuid
import random
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

CATEGORIES = ["Retail", "Electronics", "Travel", "Dining", "Services", "Health", "Entertainment", "Education", "Finance", "Real Estate", "Automotive"]

# Get configuration from config.py
PG_ENABLED = POSTGRES["ENABLED"]

# Sleep interval between merchant generation (seconds)
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

def generate_merchant():
    """Generate a merchant with random data"""
    merchant_id = str(uuid.uuid4())
    merchant_name = fake.company()
    merchant_category = random.choice(CATEGORIES)
    
    # Generate location data
    country_code = fake.country_code()
    city = fake.city()
    latitude = float(fake.latitude())
    longitude = float(fake.longitude())
    
    # Generate business details
    founded_date = fake.date_between(start_date='-30y', end_date='-1y')
    business_size = random.choice(["Small", "Medium", "Large", "Enterprise"])
    online_presence = random.choice([True, False])
    
    # Generate payment methods accepted
    payment_methods = random.sample(
        ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash", "Cryptocurrency", "Mobile Payment"],
        k=random.randint(2, 5)
    )
    
    # Generate merchant risk profile
    risk_score = random.randint(1, 100)
    fraud_history = random.choices([True, False], weights=[5, 95])[0]  # 5% chance of fraud history
    
    return {
        "merchant_id": merchant_id,
        "merchant_name": merchant_name,
        "merchant_category": merchant_category,
        "location": {
            "country": country_code,
            "city": city,
            "address": fake.street_address(),
            "postal_code": fake.postcode(),
            "latitude": latitude,
            "longitude": longitude
        },
        "business": {
            "founded_date": founded_date.isoformat(),
            "business_size": business_size,
            "online_presence": online_presence,
            "website": fake.url() if online_presence else None,
            "contact_email": fake.company_email(),
            "contact_phone": fake.phone_number()
        },
        "payment": {
            "methods_accepted": payment_methods,
            "currency": random.choice(["USD", "EUR", "GBP", "JPY", "CAD", "AUD"]),
            "average_transaction": round(random.uniform(10, 500), 2)
        },
        "risk": {
            "score": risk_score,
            "fraud_history": fraud_history,
            "verification_level": random.choice(["Basic", "Verified", "Premium"])
        }
    }

def run():
    """Main function to generate merchants and store them in PostgreSQL"""
    logger.info("🚀 Merchant producer started - writing to PostgreSQL only")
    
    # Track merchants we've already generated
    merchant_count = 0
    
    try:
        while True:
            merchant = generate_merchant()
            merchant_id = merchant["merchant_id"]
            
            # Store in PostgreSQL
            if db.insert_merchant(merchant):
                logger.info(f"✅ Stored merchant in database: {merchant_id}")
                merchant_count += 1
                
                # Every 100 merchants, log a summary
                if merchant_count % 100 == 0:
                    logger.info(f"Generated {merchant_count} merchants so far")
            else:
                logger.warning(f"❌ Failed to store merchant in database: {merchant_id}")
            
            # Sleep before generating next merchant
            time.sleep(PRODUCER_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Merchant producer stopped by user")
    except Exception as e:
        logger.error(f"Error in merchant producer: {e}", exc_info=True)
    finally:
        logger.info(f"Merchant producer finished after generating {merchant_count} merchants")

if __name__ == "__main__":
    run()

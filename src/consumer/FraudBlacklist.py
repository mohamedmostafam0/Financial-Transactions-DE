# FraudBlacklist.py

import uuid
import random
import json
from faker import Faker
from typing import List, Dict
from src.shared.consumer_cache import CacheConsumer

fake = Faker()
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Or use os.getenv()

cache = CacheConsumer(KAFKA_BOOTSTRAP_SERVERS)

def generate_fraud_blacklist(n: int) -> List[Dict[str, str]]:
    """Generate a list of blacklisted users and merchants from live cache"""
    blacklist = []

    for _ in range(n):
        cache.poll_messages()  # Continuously pull new data into cache
        entry_type = random.choice(["user", "merchant"])

        if entry_type == "user":
            user_id = cache.get_random_user()
            if not user_id:
                continue
            entry = {
                "blacklist_id": str(uuid.uuid4()),
                "entry_type": "user",
                "user_id": user_id,
                "merchant_id": None,
                "reason": fake.sentence(nb_words=6),
                "flagged_at": fake.date_time_this_year().isoformat()
            }
        else:
            merchant = cache.get_random_merchant()
            if not merchant:
                continue
            entry = {
                "blacklist_id": str(uuid.uuid4()),
                "entry_type": "merchant",
                "user_id": None,
                "merchant_id": merchant["merchant_id"],
                "reason": fake.sentence(nb_words=6),
                "flagged_at": fake.date_time_this_year().isoformat()
            }

        blacklist.append(entry)

    return blacklist


# Save to JSON
blacklist_data = generate_fraud_blacklist(100)
with open("fraud_blacklist.json", "w") as f:
    json.dump(blacklist_data, f, indent=2)

print(f"[✓] Saved {len(blacklist_data)} fraud blacklist entries to 'fraud_blacklist.json'")

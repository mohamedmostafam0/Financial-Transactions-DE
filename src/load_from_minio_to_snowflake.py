import snowflake.connector
import os

# Load credentials securely from environment variables
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = "RAW"
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_STAGE = "minio_stage"  # This should already exist in Snowflake
TARGET_TABLE = "RAW_TRANSACTIONS"

def load_from_minio():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cs = conn.cursor()

    try:
        print(f"Running COPY INTO {TARGET_TABLE} from @{SNOWFLAKE_STAGE}...")
        copy_sql = f"""
        COPY INTO {TARGET_TABLE}
        FROM @{SNOWFLAKE_STAGE}
        FILE_FORMAT = (TYPE = JSON)
        ON_ERROR = 'CONTINUE';  -- Skip bad records instead of failing
        """
        cs.execute(copy_sql)
        print("✅ Data loaded successfully from MinIO into Snowflake.")
    finally:
        cs.close()
        conn.close()

if __name__ == "__main__":
    load_from_minio()

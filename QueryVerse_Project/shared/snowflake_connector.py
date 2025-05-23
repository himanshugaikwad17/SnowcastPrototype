import snowflake.connector
from shared.config import SNOWFLAKE_CONFIG

def get_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

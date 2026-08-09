import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("RemoteDBInitializer")

def init_remote_db(db_url: str = None):
    if not db_url:
        db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5433")
        dbname = os.getenv("POSTGRES_DB", "market_db")
        user = os.getenv("POSTGRES_USER", "market_user")
        password = os.getenv("POSTGRES_PASSWORD", "market_pass")
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    logger.info(f"Connecting to database to run SQL init scripts...")
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()

        sql_files = [
            "sql/01_create_schemas.sql",
            "sql/02_bronze_tables.sql",
            "sql/03_silver_tables.sql",
            "sql/04_gold_tables.sql"
        ]

        for sql_file in sql_files:
            if os.path.exists(sql_file):
                logger.info(f"Executing {sql_file}...")
                with open(sql_file, "r") as f:
                    sql_content = f.read()
                    cur.execute(sql_content)
            else:
                logger.warning(f"File {sql_file} not found!")

        cur.close()
        conn.close()
        logger.info("Remote database initialization completed successfully!")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")

if __name__ == "__main__":
    target_url = sys.argv[1] if len(sys.argv) > 1 else None
    init_remote_db(target_url)

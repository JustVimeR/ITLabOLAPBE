from sqlalchemy import create_engine, inspect
import os
from dotenv import load_dotenv

load_dotenv()

database_url = os.getenv("DATABASE_URL")
print(f"Connecting to: {database_url}")

try:
    engine = create_engine(database_url)
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print(f"Tables found: {tables}")
except Exception as e:
    print(f"Connection failed: {e}")

from sqlalchemy import create_engine, text
import urllib.parse

server = "VimeRSPC\\SQLEXPRESS"
database = "master"
driver = "ODBC Driver 17 for SQL Server"

params = urllib.parse.quote_plus(f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};Trusted_Connection=yes")
conn_str = f"mssql+pyodbc:///?odbc_connect={params}"

engine = create_engine(conn_str, isolation_level="AUTOCOMMIT")

def create_database():
    try:
        with engine.connect() as conn:
            print(f"Connecting to {server}...")
            result = conn.execute(text("SELECT name FROM sys.databases WHERE name = 'sales_analytics'"))
            if not result.fetchone():
                print("Database 'sales_analytics' not found. Creating...")
                conn.execute(text("CREATE DATABASE sales_analytics"))
                print("Database 'sales_analytics' created successfully!")
            else:
                print("Database 'sales_analytics' already exists.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_database()

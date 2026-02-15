from database import SessionLocal
from models import FactSales

def check_upload():
    db = SessionLocal()
    try:
        sale = db.query(FactSales).filter_by(sale_id=9001).first()
        if sale:
            print(f"SUCCESS: Found sale_id {sale.sale_id}, Revenue: {sale.revenue}")
        else:
            print("FAILURE: Sale_id 9001 not found.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    check_upload()

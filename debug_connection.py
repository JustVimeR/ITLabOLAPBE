from database import SessionLocal
import models
import sys
import os

sys.path.append(os.getcwd())

def test_connection():
    print("Testing database connection...")
    db = SessionLocal()
    try:
        print("Querying FactSales...")
        sale = db.query(models.FactSales).first()
        if sale:
            print(f"Success! Found sale with ID: {sale.sale_id}")
        else:
            print("Success! Connection works, but FactSales table is empty.")
            
        print("Querying DimProduct...")
        product = db.query(models.DimProduct).first()
        if product:
            print(f"Success! Found product: {product.name}")
        else:
            print("Success! Connection works, but DimProduct table is empty.")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    test_connection()

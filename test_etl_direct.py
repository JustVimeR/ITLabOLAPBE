import sys
import os
sys.path.append(os.getcwd())

import pandas as pd
import etl
from database import SessionLocal

def test_etl():
    print("Testing ETL logic directly...")
    try:
        # Create sample DataFrame
        data = {
            'sale_id': [9004],
            'sale_datetime': ['2023-12-03'],
            'region_name': ['West'],
            'city': ['Lannisport'],
            'manager': ['Tyrion'],
            'product_id': [103],
            'product_name': ['Wine'],
            'brand': ['Lannister'],
            'category': ['Beverages'],
            'supplier_name': ['Casterly Rock'],
            'supplier_country': ['Westeros'],
            'quantity': [10],
            'unit_price': [50],
            'discount': [5],
            # 'revenue': [495], # Missing revenue, should be calculated: 10*50 - 5 = 495
            'payment_type': ['Gold'],
            'sales_channel': ['Direct']
        }
        df = pd.DataFrame(data)
        
        db = SessionLocal()
        result = etl.process_data(df, db)
        print("ETL Result:", result)
        db.close()
        print("SUCCESS_DIRECT_ETL")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_etl()

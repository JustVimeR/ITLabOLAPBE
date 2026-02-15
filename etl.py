import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from database import SessionLocal, engine
import models
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime

def load_data(file_path: str = "fact_sales_diverse.csv"):
    db = SessionLocal()
    try:
        df = pd.read_csv(file_path)
        
        # 1. Dimensions
        # Region
        regions = df[['region_name', 'city']].drop_duplicates()
        for _, row in regions.iterrows():
            exists = db.query(models.DimRegion).filter_by(region_name=row['region_name'], city=row['city']).first()
            if not exists:
                db.add(models.DimRegion(region_name=row['region_name'], city=row['city']))
        db.commit()

        # Manager
        managers = df['manager'].unique()
        for name in managers:
            if not db.query(models.DimManager).filter_by(name=name).first():
                db.add(models.DimManager(name=name))
        db.commit()

        # Supplier
        suppliers = df[['supplier_name', 'supplier_country']].drop_duplicates()
        for _, row in suppliers.iterrows():
            if not db.query(models.DimSupplier).filter_by(name=row['supplier_name']).first():
                db.add(models.DimSupplier(name=row['supplier_name'], country=row['supplier_country']))
        db.commit()

        # Category
        categories = df['category'].unique()
        for cat in categories:
            if not db.query(models.DimCategory).filter_by(name=cat).first():
                db.add(models.DimCategory(name=cat))
        db.commit()
        
        # Product
        # Need category map
        cat_map = {c.name: c.id for c in db.query(models.DimCategory).all()}
        products = df[['product_id', 'product_name', 'brand', 'category']].drop_duplicates(subset=['product_id'])
        for _, row in products.iterrows():
            if not db.query(models.DimProduct).filter_by(business_id=row['product_id']).first():
                db.add(models.DimProduct(
                    business_id=row['product_id'],
                    name=row['product_name'],
                    brand=row['brand'],
                    category_id=cat_map.get(row['category'])
                ))
        db.commit()

        # Date
        # Convert sale_datetime to date
        df['date'] = pd.to_datetime(df['sale_datetime']).dt.date
        dates = df['date'].unique()
        for d in dates:
            if not db.query(models.DimDate).filter_by(date=d).first():
                db.add(models.DimDate(
                    date=d,
                    year=d.year,
                    quarter=(d.month - 1) // 3 + 1,
                    month=d.month,
                    day=d.day,
                    month_name=d.strftime('%B'),
                    day_name=d.strftime('%A')
                ))
        db.commit()

        # 2. Re-map IDs for Fact Table
        # Load all dims into memory for fast lookup
        region_map = {(r.region_name, r.city): r.id for r in db.query(models.DimRegion).all()}
        manager_map = {m.name: m.id for m in db.query(models.DimManager).all()}
        supplier_map = {s.name: s.id for s in db.query(models.DimSupplier).all()}
        product_map = {p.business_id: p.id for p in db.query(models.DimProduct).all()}
        date_map = {d.date: d.id for d in db.query(models.DimDate).all()}

        # 3. Facts
        # Filter existing (by sale_id)
        existing_ids = {s.sale_id for s in db.query(models.FactSales.sale_id).all()}
        
        sales_to_insert = []
        for index, row in df.iterrows():
            if row['sale_id'] in existing_ids:
                continue

            sale = models.FactSales(
                sale_id=row['sale_id'],
                date_id=date_map.get(row['date']),
                product_id=product_map.get(row['product_id']),
                manager_id=manager_map.get(row['manager']),
                supplier_id=supplier_map.get(row['supplier_name']),
                region_id=region_map.get((row['region_name'], row['city'])),
                quantity=row['quantity'],
                unit_price=row['unit_price'],
                discount=row['discount'],
                revenue=row['revenue'],
                payment_type=row['payment_type'],
                sales_channel=row['sales_channel']
            )
            sales_to_insert.append(sale)
        
        db.add_all(sales_to_insert)
        db.commit()

        return {"message": "Success", "rows_processed": len(df), "rows_inserted": len(sales_to_insert)}

    except Exception as e:
        db.rollback()
        print(f"Error loading data: {e}")
        raise e
    finally:
        db.close()

if __name__ == "__main__":
    print("Starting ETL...")
    result = load_data()
    print(f"ETL Finished: {result}")

from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Any
import models
import schemas
import crud
import etl
from database import engine, get_db

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Sales Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/etl/load", response_model=schemas.ETLResult)
def run_etl():
    try:
        result = etl.load_data()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload/sales")
async def upload_sales(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload an Excel file.")
    
    try:
        contents = await file.read()
        import pandas as pd
        import io
        
        df = pd.read_excel(io.BytesIO(contents))
        
        # Ensure column names map to what process_data expects
        # We might need to map user columns to our expected columns
        # For now assuming user provides correct headers or we map common ones
        
        # Basic mapping if needed (optional)
        # df.rename(columns={'Region': 'region_name', ...}, inplace=True)
        
        result = etl.process_data(df, db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")

@app.post("/sales", response_model=schemas.Sale)
def create_sale(sale: schemas.SaleCreate, db: Session = Depends(get_db)):
    return crud.create_sale(db=db, sale=sale)

@app.get("/sales", response_model=List[schemas.Sale])
def read_sales(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    sales = crud.get_sales(db, skip=skip, limit=limit)
    return sales

@app.put("/sales/{sale_id}", response_model=schemas.Sale)
def update_sale(sale_id: int, sale: schemas.SaleCreate, db: Session = Depends(get_db)):
    db_sale = crud.update_sale(db, sale_id, sale)
    if not db_sale:
        raise HTTPException(status_code=404, detail="Sale not found")
    return db_sale

@app.delete("/sales/{sale_id}")
def delete_sale(sale_id: int, db: Session = Depends(get_db)):
    db_sale = crud.delete_sale(db, sale_id)
    if not db_sale:
        raise HTTPException(status_code=404, detail="Sale not found")
    return {"message": "Success"}

@app.get("/dims/{dim_name}", response_model=List[schemas.DimBase])
def read_dims(dim_name: str, db: Session = Depends(get_db)):
    return crud.get_dims(db, dim_name)


@app.get("/rankings/{entity_type}")
def read_rankings(entity_type: str, limit: int = 5, db: Session = Depends(get_db)):
    return crud.get_rankings(db, entity_type, limit)

@app.get("/reports/aggregate")
def read_aggregate_report(
    dimension1: str,
    dimension2: str,
    metric: str = 'revenue',
    db: Session = Depends(get_db)
):
    dim_map = {
        'region': models.DimRegion.region_name,
        'manager': models.DimManager.name,
        'category': models.DimCategory.name,
        'product': models.DimProduct.name,
        'supplier': models.DimSupplier.name,
        'year': models.DimDate.year,
        'quarter': models.DimDate.quarter,
        'month': models.DimDate.month_name,
    }
    
    col1 = dim_map.get(dimension1)
    col2 = dim_map.get(dimension2)

    if not col1 or not col2:
        raise HTTPException(status_code=400, detail="Invalid dimension")

    q = db.query(
        col1.label("dim1"),
        col2.label("dim2"),
        func.sum(models.FactSales.revenue).label("value")
    ).select_from(models.FactSales)

    
    q = q.join(models.DimDate).join(models.DimProduct).join(models.DimRegion).join(models.DimManager).join(models.DimSupplier)

    q = q.join(models.DimCategory, models.DimProduct.category_id == models.DimCategory.id)

    q = q.group_by(col1, col2).all()
    
    return [{"d1": r[0], "d2": r[1], "value": float(r[2] or 0)} for r in q]

@app.get("/dashboard/metrics")
def read_dashboard_metrics(db: Session = Depends(get_db)):
    total_revenue = db.query(func.sum(models.FactSales.revenue)).scalar() or 0
    total_quantity = db.query(func.sum(models.FactSales.quantity)).scalar() or 0
    count_sales = db.query(func.count(models.FactSales.id)).scalar() or 0
    avg_check = total_revenue / count_sales if count_sales > 0 else 0
    
    return {
        "total_revenue": total_revenue,
        "total_quantity": total_quantity,
        "count_sales": count_sales,
        "avg_check": avg_check
    }

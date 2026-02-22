from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from models import FactSales, DimManager, DimProduct, DimRegion, DimSupplier, DimCategory, DimDate
import schemas

def get_sale(db: Session, sale_id: int):
    return db.query(FactSales).filter(FactSales.id == sale_id).first()

def get_sales(db: Session, skip: int = 0, limit: int = 100, search: str = None):
    q = db.query(FactSales)
    if search:
        q = q.join(DimProduct, FactSales.product_id == DimProduct.id)\
             .join(DimManager, FactSales.manager_id == DimManager.id)\
             .join(DimRegion, FactSales.region_id == DimRegion.id)
        like = f"%{search}%"
        q = q.filter(
            (DimProduct.name.ilike(like)) |
            (DimManager.name.ilike(like)) |
            (DimRegion.region_name.ilike(like)) |
            (DimRegion.city.ilike(like))
        )
    return q.order_by(FactSales.id).offset(skip).limit(limit).all()

def get_sales_count(db: Session, search: str = None):
    q = db.query(func.count(FactSales.id))
    if search:
        q = q.join(DimProduct, FactSales.product_id == DimProduct.id)\
             .join(DimManager, FactSales.manager_id == DimManager.id)\
             .join(DimRegion, FactSales.region_id == DimRegion.id)
        like = f"%{search}%"
        q = q.filter(
            (DimProduct.name.ilike(like)) |
            (DimManager.name.ilike(like)) |
            (DimRegion.region_name.ilike(like)) |
            (DimRegion.city.ilike(like))
        )
    return q.scalar()

def create_sale(db: Session, sale: schemas.SaleCreate):
    
    revenue = (sale.quantity * sale.unit_price) - sale.discount
    
    max_id = db.query(func.max(FactSales.sale_id)).scalar() or 0
    new_sale_id = max_id + 1

    db_sale = FactSales(
        sale_id=new_sale_id,
        date_id=1, 
        product_id=sale.product_id,
        manager_id=sale.manager_id,
        supplier_id=sale.supplier_id,
        region_id=sale.region_id,
        quantity=sale.quantity,
        unit_price=sale.unit_price,
        discount=sale.discount,
        revenue=revenue,
        payment_type=sale.payment_type,
        sales_channel=sale.sales_channel
    )
    
    db.add(db_sale)
    db.commit()
    db.refresh(db_sale)
    return db_sale

def update_sale(db: Session, sale_id: int, sale: schemas.SaleCreate):
    db_sale = db.query(FactSales).filter(FactSales.id == sale_id).first()
    if not db_sale:
        return None
    
    # Update fields
    db_sale.product_id = sale.product_id
    db_sale.manager_id = sale.manager_id
    db_sale.supplier_id = sale.supplier_id
    db_sale.region_id = sale.region_id
    db_sale.quantity = sale.quantity
    db_sale.unit_price = sale.unit_price
    db_sale.discount = sale.discount
    db_sale.payment_type = sale.payment_type
    db_sale.sales_channel = sale.sales_channel
    
    # Recalculate revenue
    db_sale.revenue = (sale.quantity * sale.unit_price) - sale.discount

    db.commit()
    db.refresh(db_sale)
    return db_sale

def delete_sale(db: Session, sale_id: int):
    db_sale = db.query(FactSales).filter(FactSales.id == sale_id).first()
    if db_sale:
        db.delete(db_sale)
        db.commit()
    return db_sale

def get_dims(db: Session, dim_name: str):
    if dim_name == "manager":
        return db.query(DimManager).all()
    if dim_name == "category":
        return db.query(DimCategory).all()
    if dim_name == "product":
        return db.query(DimProduct).all()
    if dim_name == "region":
        return db.query(DimRegion).all()
    if dim_name == "supplier":
        return db.query(DimSupplier).all()
    return []


def get_rankings(db: Session, entity_type: str, limit: int = 5):
    if entity_type == "manager":
        model = DimManager
        col = FactSales.manager_id
        name_col = DimManager.name
    elif entity_type == "product":
        model = DimProduct
        col = FactSales.product_id
        name_col = DimProduct.name
    elif entity_type == "region":
        model = DimRegion
        col = FactSales.region_id
        name_col = DimRegion.region_name 
    else:
        return []

    results = db.query(
        name_col.label("name"),
        func.sum(FactSales.revenue).label("total_revenue")
    ).join(model, col == model.id)\
    .group_by(name_col)\
    .order_by(desc("total_revenue"))\
    .limit(limit)\
    .all()
    
    return [{"rank": i+1, "name": r[0], "revenue": float(r[1])} for i, r in enumerate(results)]

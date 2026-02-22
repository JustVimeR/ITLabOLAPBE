from sqlalchemy.orm import Session
from sqlalchemy import func
from models import OltpSale
import schemas
import pandas as pd
import etl


def get_oltp_sales(db: Session, skip: int = 0, limit: int = 20):
    return db.query(OltpSale).order_by(OltpSale.id.desc()).offset(skip).limit(limit).all()


def get_oltp_sales_count(db: Session):
    return db.query(func.count(OltpSale.id)).scalar()


def create_oltp_sale(db: Session, sale: schemas.OltpSaleCreate):
    # Auto-calculate revenue if not provided
    revenue = sale.revenue if sale.revenue is not None else (sale.quantity * sale.unit_price - sale.discount)
    
    # Auto-generate sale_id if not provided
    sale_id = sale.sale_id
    if sale_id is None:
        max_id = db.query(func.max(OltpSale.sale_id)).scalar() or 0
        sale_id = max_id + 1
    
    # Auto-generate product_id if not provided
    product_id = sale.product_id
    if product_id is None:
        max_pid = db.query(func.max(OltpSale.product_id)).scalar() or 0
        product_id = max_pid + 1
    
    db_sale = OltpSale(
        sale_id=sale_id,
        sale_datetime=sale.sale_datetime,
        region_name=sale.region_name,
        city=sale.city,
        manager=sale.manager,
        product_id=product_id,
        product_name=sale.product_name,
        brand=sale.brand,
        category=sale.category,
        supplier_name=sale.supplier_name,
        supplier_country=sale.supplier_country,
        quantity=sale.quantity,
        unit_price=sale.unit_price,
        discount=sale.discount,
        revenue=revenue,
        payment_type=sale.payment_type,
        sales_channel=sale.sales_channel,
        transferred=0,
    )
    db.add(db_sale)
    db.commit()
    db.refresh(db_sale)
    return db_sale


def update_oltp_sale(db: Session, sale_id: int, sale: schemas.OltpSaleCreate):
    db_sale = db.query(OltpSale).filter(OltpSale.id == sale_id).first()
    if not db_sale:
        return None
    
    for key, value in sale.model_dump().items():
        setattr(db_sale, key, value)
    
    # Recalculate revenue
    db_sale.revenue = sale.revenue if sale.revenue is not None else (sale.quantity * sale.unit_price - sale.discount)
    
    db.commit()
    db.refresh(db_sale)
    return db_sale


def delete_oltp_sale(db: Session, sale_id: int):
    db_sale = db.query(OltpSale).filter(OltpSale.id == sale_id).first()
    if db_sale:
        db.delete(db_sale)
        db.commit()
    return db_sale


def transfer_to_warehouse(db: Session, ids: list[int]):
    """Transfer selected OLTP records to the OLAP warehouse."""
    records = db.query(OltpSale).filter(
        OltpSale.id.in_(ids),
        OltpSale.transferred == 0
    ).all()
    
    if not records:
        return {"message": "No records to transfer", "rows_processed": 0, "rows_inserted": 0}
    
    # Convert to DataFrame matching ETL expected format
    data = []
    for r in records:
        data.append({
            'sale_id': r.sale_id,
            'sale_datetime': r.sale_datetime,
            'region_name': r.region_name,
            'city': r.city,
            'manager': r.manager,
            'product_id': r.product_id,
            'product_name': r.product_name,
            'brand': r.brand,
            'category': r.category,
            'supplier_name': r.supplier_name,
            'supplier_country': r.supplier_country,
            'quantity': r.quantity,
            'unit_price': float(r.unit_price),
            'discount': float(r.discount),
            'revenue': float(r.revenue) if r.revenue else None,
            'payment_type': r.payment_type,
            'sales_channel': r.sales_channel,
        })
    
    df = pd.DataFrame(data)
    result = etl.process_data(df, db)
    
    # Mark as transferred
    for r in records:
        r.transferred = 1
    db.commit()
    
    return result

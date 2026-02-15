from pydantic import BaseModel
from typing import Optional, List
from datetime import date

# Dimensions
class DimBase(BaseModel):
    id: int
    name: str

    class Config:
        from_attributes = True

class DimProduct(BaseModel):
    id: int
    name: str
    category: str

    class Config:
        from_attributes = True

# Sales
class SaleBase(BaseModel):
    date: date
    product_id: int
    manager_id: int
    supplier_id: int
    region_id: int
    quantity: int
    unit_price: float
    discount: float = 0
    payment_type: str
    sales_channel: str

class SaleCreate(SaleBase):
    pass

class Sale(SaleBase):
    id: int
    sale_id: int
    revenue: float

    class Config:
        from_attributes = True

class SalesReport(BaseModel):
    total_revenue: float
    total_quantity: int
    count: int

class RankingItem(BaseModel):
    rank: int
    name: str
    revenue: float

class ETLResult(BaseModel):
    message: str
    rows_processed: int
    rows_inserted: int

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

# OLTP
class OltpSaleCreate(BaseModel):
    sale_id: Optional[int] = None
    sale_datetime: str
    region_name: str
    city: str
    manager: str
    product_id: Optional[int] = None
    product_name: str
    brand: Optional[str] = None
    category: str
    supplier_name: str
    supplier_country: Optional[str] = None
    quantity: int
    unit_price: float
    discount: float = 0
    revenue: Optional[float] = None
    payment_type: Optional[str] = None
    sales_channel: Optional[str] = None

class OltpSaleResponse(OltpSaleCreate):
    id: int
    transferred: int = 0

    class Config:
        from_attributes = True

class TransferRequest(BaseModel):
    ids: List[int]

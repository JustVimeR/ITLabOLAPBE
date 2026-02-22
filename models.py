from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Numeric
from sqlalchemy.orm import relationship
from database import Base

class DimDate(Base):
    __tablename__ = "dim_date"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, unique=True, nullable=False)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    month_name = Column(String(20), nullable=False)
    day_name = Column(String(20), nullable=False)

class DimCategory(Base):
    __tablename__ = "dim_category"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)
    
    products = relationship("DimProduct", back_populates="category")

class DimProduct(Base):
    __tablename__ = "dim_product"

    id = Column(Integer, primary_key=True, index=True)
    business_id = Column(Integer, nullable=True) 
    name = Column(String(255), nullable=False)
    brand = Column(String(100), nullable=True)
    category_id = Column(Integer, ForeignKey("dim_category.id"))

    category = relationship("DimCategory", back_populates="products")

class DimManager(Base):
    __tablename__ = "dim_manager"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)

class DimSupplier(Base):
    __tablename__ = "dim_supplier"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    country = Column(String(100), nullable=True)

class DimRegion(Base):
    __tablename__ = "dim_region"

    id = Column(Integer, primary_key=True, index=True)
    region_name = Column(String(100), nullable=False)
    city = Column(String(100), nullable=False)

    @property
    def name(self):
        return f"{self.region_name} - {self.city}"

class FactSales(Base):
    __tablename__ = "fact_sales"

    id = Column(Integer, primary_key=True, index=True)
    sale_id = Column(Integer, unique=True, nullable=False) 
    
    date_id = Column(Integer, ForeignKey("dim_date.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("dim_product.id"), nullable=False)
    manager_id = Column(Integer, ForeignKey("dim_manager.id"), nullable=False)
    supplier_id = Column(Integer, ForeignKey("dim_supplier.id"), nullable=False)
    region_id = Column(Integer, ForeignKey("dim_region.id"), nullable=False)

    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    discount = Column(Numeric(10, 2), default=0)
    revenue = Column(Numeric(10, 2), nullable=False)
    
    payment_type = Column(String(50), nullable=True)
    sales_channel = Column(String(50), nullable=True)

    # Relationships
    dim_date = relationship("DimDate")
    product = relationship("DimProduct")
    manager = relationship("DimManager")
    supplier = relationship("DimSupplier")
    region = relationship("DimRegion")

    @property
    def date(self):
        return self.dim_date.date if self.dim_date else None

class OltpSale(Base):
    __tablename__ = "oltp_sales"

    id = Column(Integer, primary_key=True, index=True)
    sale_id = Column(Integer, nullable=False)
    sale_datetime = Column(String(50), nullable=False)
    region_name = Column(String(100), nullable=False)
    city = Column(String(100), nullable=False)
    manager = Column(String(100), nullable=False)
    product_id = Column(Integer, nullable=False)
    product_name = Column(String(255), nullable=False)
    brand = Column(String(100), nullable=True)
    category = Column(String(100), nullable=False)
    supplier_name = Column(String(100), nullable=False)
    supplier_country = Column(String(100), nullable=True)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    discount = Column(Numeric(10, 2), default=0)
    revenue = Column(Numeric(10, 2), nullable=True)
    payment_type = Column(String(50), nullable=True)
    sales_channel = Column(String(50), nullable=True)
    transferred = Column(Integer, default=0)  # 0 = not transferred, 1 = transferred

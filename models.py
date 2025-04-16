from sqlalchemy import (
    Column, 
    Integer, 
    String, 
    DECIMAL,
    Boolean,
    Date,
    ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship

"""
SQLAlchemy script that creates ORMs for interacting with the database.
In effect, this script resembles the database shema.
initiating Alembic on this program will create the tables in the 
database according to this schema (read the README for more detailed instructions).
"""

Base = declarative_base()

class DailyData(Base):
    __tablename__ = "daily_data"

    date = Column(Date, index=True)
    store_id = Column(Integer, ForeignKey("stores.store_id"), index=True)
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String(255), nullable=False)
    has_bio_label = Column(Boolean, nullable=False, default=False)
    category_id = Column(ForeignKey("categories.category_id"), nullable=False, index=True)
    listed_price = Column(DECIMAL(10,2))
    listed_amount = Column(DECIMAL(10,2))
    listed_unit = Column(String(10))
    is_on_offer = Column(Boolean, nullable=False)

class Stores(Base):
    __tablename__ = "stores"

    store_id = Column(Integer, autoincrement=True, primary_key=True, index=True)
    store_name = Column(String(255), nullable=False, unique=True)

    observations = relationship("ProductObservations", back_populates="store")
    
class Categories(Base):
    __tablename__ = "categories" 

    category_id = Column(Integer, primary_key=True, autoincrement=True)
    category_name = Column(String(100), unique=True)

    product = relationship("Products", back_populates="category")

class Products(Base):
    __tablename__ = "products" 

    product_id = Column(Integer, primary_key=True)
    product_name = Column(String(255), nullable=False)
    has_bio_label = Column(Boolean, nullable=False, default=False)
    category_id = Column(ForeignKey("categories.category_id"), nullable=False, index=True)

    category = relationship("Categories", back_populates="product")
    observations = relationship("ProductObservations", back_populates="product")

class ProductObservations(Base):
    __tablename__ = "product_observations" 

    observation_id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(Integer, ForeignKey("stores.store_id"), index=True)
    product_id = Column(Integer, ForeignKey("products.product_id"), index=True)
    date = Column(Date, nullable=False, index=True)
    listed_price = Column(DECIMAL(10,2))
    listed_amount = Column(DECIMAL(10,2))
    listed_unit = Column(String(10))
    is_on_offer = Column(Boolean, nullable=False)
    is_available = Column(Boolean, nullable=False)

    store = relationship("Stores", back_populates="observations")
    product = relationship("Products", back_populates="observations")

class DailyStatistics(Base):
    __tablename__ = "daily_statistics"

    date = Column(Date, index=True)
    store_id = Column(Integer, ForeignKey("stores.store_id"), primary_key=True, index=True)

    price_mean = Column(DECIMAL(10, 4))
    price_median = Column(DECIMAL(10, 4))
    amount_bio_products = Column(Integer)
    amount_reduced_products = Column(Integer)
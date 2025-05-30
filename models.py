from sqlalchemy import (
    Column, 
    Integer, 
    String, 
    DECIMAL,
    Boolean,
    Date,
    ForeignKey,
    PrimaryKeyConstraint
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
    __table_args__ = (
        PrimaryKeyConstraint('date', 'store_id', 'product_id'),
    )

    date = Column(Date, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("stores.store_id"), primary_key=True, index=True)
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
    is_available = Column(Boolean, nullable=False, default=True)

    store = relationship("Stores", back_populates="observations")
    product = relationship("Products", back_populates="observations")

class DailyStatistics(Base):
    __tablename__ = "daily_statistics"

    date = Column(Date, index=True, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.store_id"), index=True, primary_key=True)

    price_min = Column(DECIMAL(10, 4)) ## lowest price in dataset
    price_max = Column(DECIMAL(10, 4)) ## highest price in dataset
    price_mean = Column(DECIMAL(10, 4)) ## average price in dataset
    price_median = Column(DECIMAL(10, 4)) ## middle price in dataset
    price_skewness = Column(DECIMAL(5, 3)) ## asymmetry of the distribution of price points
    price_standard_deviation = Column(DECIMAL(10, 4)) ## how much prices typically deviate from the mean
    price_variance = Column(DECIMAL(10, 4)) ## the square of the standard deviation
    price_range = Column(DECIMAL(10, 4)) ## difference between the lowest listed price and the highest
    price_quartile_1 = Column(DECIMAL(10, 4)) ## the price of products at the 25th percentile (cheaper than 75% of the rest)
    price_quartile_3 = Column(DECIMAL(10, 4)) ## the price of products at the 75th percentile (more expensive than 75% of the rest)
    IQR = Column(DECIMAL(10, 4)) ## difference between the 75th and 25th percentiles


    amount_total_products = Column(Integer)
    amount_bio_products = Column(Integer)
    amount_reduced_products = Column(Integer)
    percentage_bio_products = Column(DECIMAL(10, 4), nullable=True)
    percentage_reduced_products = Column(DECIMAL(10, 4))
    
class CategoryStatistics(Base):
    __tablename__ = "category_statistics"
    __table_args__ = (
        PrimaryKeyConstraint('date', 'store_id', 'category_id'),
    )

    date = Column(Date, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("stores.store_id"), primary_key=True, index=True)
    category_id = Column(Integer, ForeignKey("categories.category_id"), primary_key=True, nullable=False, index=True)

    price_min = Column(DECIMAL(10, 4)) ## lowest price in dataset
    price_max = Column(DECIMAL(10, 4)) ## highest price in dataset
    price_mean = Column(DECIMAL(10, 4)) ## average price in dataset
    price_median = Column(DECIMAL(10, 4)) ## middle price in dataset
    price_skewness = Column(DECIMAL(5, 3)) ## asymmetry of the distribution of price points
    price_standard_deviation = Column(DECIMAL(10, 4)) ## how much prices typically deviate from the mean
    price_variance = Column(DECIMAL(10, 4)) ## the square of the standard deviation
    price_range = Column(DECIMAL(10, 4)) ## difference between the lowest listed price and the highest
    price_quartile_1 = Column(DECIMAL(10, 4)) ## the price of products at the 25th percentile (cheaper than 75% of the rest)
    price_quartile_3 = Column(DECIMAL(10, 4)) ## the price of products at the 75th percentile (more expensive than 75% of the rest)
    IQR = Column(DECIMAL(10, 4)) ## difference between the 75th and 25th percentiles

    amount_total_products = Column(Integer)
    amount_bio_products = Column(Integer)
    amount_reduced_products = Column(Integer)
    percentage_bio_products = Column(DECIMAL(10, 4), nullable=True)
    percentage_reduced_products = Column(DECIMAL(10, 4))
    
    green_premium = Column(DECIMAL(10, 4), nullable=True) ## average price difference between the average product with a bio label relative to the median price
    average_savings = Column(DECIMAL(10, 4)) ## average price difference between the average product with a reduced price relative to the median price
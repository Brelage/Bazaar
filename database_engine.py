from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from os import getenv

try:
    engine = create_engine(getenv("DATABASE_URL"))
except ValueError as e:
    raise f"{e} Error. Database_URL not found in environment variable."

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
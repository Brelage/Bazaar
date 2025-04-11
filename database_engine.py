from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from os import getenv
from sys import exit


try:
    DATABASE_URL = getenv("DATABASE_URL")    
except ValueError as e:
    print(f"{e} Error. Database_URL not found in environment variable.")
    exit(1)
    
engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
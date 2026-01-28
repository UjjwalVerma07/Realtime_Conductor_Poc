import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,declarative_base

DATABASE_URL=os.getenv("DATABASE_URL")

engine=create_engine(DATABASE_URL,pool_pre_ping=True) #Creates the database engine
SessionLocal=sessionmaker(autocommit=False,autoflush=False,bind=engine) #Factory for creating the database sessions
Base=declarative_base() #Base class for creating ORM models/tables


def get_db():
    db=SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
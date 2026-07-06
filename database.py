from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,declarative_base
import os
# Create a connection string
DATABASE_URL = ("postgresql+psycopg2://postgres:password@localhost:5432/prefect")

# DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    echo=True
)

SessionLocal = sessionmaker(
    bind=engine
)

session = SessionLocal()
Base = declarative_base()
# Base.metadata.create_all(engine)
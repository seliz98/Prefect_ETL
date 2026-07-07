from sqlalchemy import Column, Integer, String,DateTime
from datetime import datetime,timezone
from database import SessionLocal,Base


# Base = declarative_base()
class Articles(Base):
    __tablename__ = "articles"

    id = Column(Integer, primary_key=True)

    title = Column(String(500))

    published_at = Column(DateTime(timezone=True))

    url = Column(String(900))

    comments_count = Column(Integer)

    positive_reactions_count = Column(Integer)

    tag_list = Column(String(900))

    username = Column(String(100))


    
    load_timestamp = Column(DateTime(timezone=True),default=lambda: datetime.now(timezone.utc))




class ETLMetadata(Base):
    __tablename__ = "etl_metadata"

    id = Column(Integer, primary_key=True)

    pipeline_name = Column(String(100))

    run_start_time = Column(DateTime(timezone=True))

    run_end_time = Column(DateTime(timezone=True))

    status = Column(String(50))

    records_extracted = Column(Integer)

    records_loaded = Column(Integer)

    watermark = Column(DateTime(timezone=True))

    error_message = Column(String(1000), nullable=True)

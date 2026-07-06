from database import engine, Base
from models import Articles, ETLMetadata

Base.metadata.create_all(engine)

print("Tables created")
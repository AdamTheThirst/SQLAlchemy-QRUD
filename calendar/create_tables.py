from database import sync_engine

from database import Base

def create_tables():
    Base.metadata.drop_all(bind=sync_engine)
    Base.metadata.create_all(bind=sync_engine)
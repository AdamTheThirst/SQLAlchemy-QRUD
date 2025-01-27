import asyncio
from typing import Annotated

from sqlalchemy import create_engine, text, insert, String
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase

from config import *

sync_engine = create_engine(
    url = settings.DATABASE_URL_psycopg,
    echo = True,
    pool_size = 5,
    max_overflow = 10,
)

async_engine = create_async_engine(
    url = settings.DATABASE_URL_asyncpg,
    echo = False,
    pool_size = 5,
    max_overflow = 10,
)

sync_session_fabric = sessionmaker(sync_engine)
async_session_fabric = async_sessionmaker(async_engine)

str_200 = Annotated[str, 200]

class Base(DeclarativeBase):
    type_annotation_map = {
        str_200: String(200)
    }
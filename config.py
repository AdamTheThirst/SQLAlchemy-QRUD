from pydantic_settings import *

class Settings(BaseSettings):
    LOCAL_HOST: str
    DB_HOST: str
    DB_PORT: str
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str

    model_config = SettingsConfigDict(env_file='.env')

    @property
    def DATABASE_URL_psycopg(self):
        #postrgresql+psycopg://user:password@localhost:port/db_namepython -m pip install --upgrade pip
        return f'postgresql+psycopg://{self.DB_USER}:{self.DB_PASSWORD}@{self.LOCAL_HOST}:{self.DB_PORT}/{self.DB_NAME}'

    @property
    def DATABASE_URL_asyncpg(self):
        #postrgresql+psycopg://user:password@localhost:port/db_namepython -m pip install --upgrade pip
        return f'postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.LOCAL_HOST}:{self.DB_PORT}/{self.DB_NAME}'

settings = Settings()

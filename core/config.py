import os
from dotenv import load_dotenv
from utils.log import setup_logger

load_dotenv()

logger = setup_logger(__name__)

class Settings:
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    SSL_MODE = os.getenv("SSL_MODE")
    DB_DRIVER = 'asyncpg'

    @property
    def DATABASE_URL(self):
        if not all([self.DB_USER, self.DB_PASSWORD, self.DB_HOST, self.DB_PORT, self.DB_NAME]):
            logger.danger("Database credentials are not set")
            raise ValueError("Database credentials are not set")
        if self.DB_DRIVER == "asyncpg":
            return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        else:
            return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}?sslmode={self.SSL_MODE}"
    @property
    def DB_CONFIG(self):
        required_fields = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME", "SSL_MODE"]
        if not all(getattr(self, field) for field in required_fields):
            logger.danger("Database credentials are not set")
            raise ValueError("Database credentials are not set")
        return {
            "user": self.DB_USER,
            "password": self.DB_PASSWORD,
            "host": self.DB_HOST,
            "port": self.DB_PORT,
            "DATABASE_URL": self.DATABASE_URL
        }

settings = Settings()

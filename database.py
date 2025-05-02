from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from core.config import settings
from utils.log import setup_logger
from models.models import Base
from sqlalchemy import text

logger = setup_logger(__name__)

# Create async engine
async_engine = create_async_engine(
    settings.DATABASE_URL,
    # echo=True,  # Uncomment for SQL logging
    future=True
)

# Create async session factory
async_session = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def init_db():
    """Initialize the database by creating all tables"""
    try:
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise e
    
async def get_db():
    """Get a database session"""
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()

async def check_db_connection():
    """Check if the database connection is working
    
    Returns:
        bool: True if the connection is working, False otherwise
    """
    try:
        async with async_engine.connect() as conn:
            # Use text() to create an executable SQL statement
            result = await conn.execute(text("SELECT 1"))
            # Fetch the result to ensure the query executed successfully
            row = result.scalar()
            return row == 1
    except Exception as e:
        logger.error(f"Database connection check failed: {e}")
        return False
    
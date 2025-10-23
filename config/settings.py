import os
from typing import Dict, Any, Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv("./../.env", override=True)

logger = logging.getLogger(__name__)

class DatabaseSettings(BaseSettings):
    """Central database connection settings"""
    user: str = Field(
        description="Database username"
    )
    password: str = Field(
        description="Database password"
    )
    host: str = Field(
        description="Database host"
    )
    port: str = Field(
        default="5432",
        description="Database port"
    )
    name: str = Field(
        description="Database name"
    )
    pool_size: int = Field(
        default=5,
        description="Database connection pool size"
    )
    max_overflow: int = Field(
        default=10,
        description="Maximum overflow connections"
    )
    echo: bool = Field(
        default=False,
        description="Echo SQL statements"
    )

    class Config:
        env_prefix = "DB_"

    @property
    def url(self) -> str:
        """Get the complete database URL"""
        if not all([self.user, self.password, self.host, self.port, self.name]):
            raise ValueError("Missing required environment variables for central database")
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

class RedisSettings(BaseSettings):
    """Redis configuration settings"""
    host: str = Field(
        default="localhost",
        description="Redis host"
    )
    port: int = Field(
        default=6379,
        description="Redis port"
    )
    db: int = Field(
        default=0,
        description="Redis database number"
    )
    password: Optional[str] = Field(
        default=None,
        description="Redis password"
    )

    class Config:
        env_prefix = "REDIS_"

class JWTSettings(BaseSettings):
    """JWT authentication settings"""
    secret: str = Field(
        default="your_secret_key",
        description="JWT secret key"
    )
    algorithm: str = Field(
        default="HS256",
        description="JWT algorithm"
    )
    expire_minutes: int = Field(
        default=30,
        description="JWT token expiration in minutes"
    )

    class Config:
        env_prefix = "JWT_"

class LoggingSettings(BaseSettings):
    """Logging configuration"""
    level: str = Field(
        default="INFO",
        description="Logging level"
    )
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format"
    )
    file_path: Optional[str] = Field(
        default=None,
        description="Log file path (if None, logs to console only)"
    )
    log_to_console: bool = Field(
        default=True,
        description="Log to console"
    )
    log_to_file: bool = Field(
        default=False,
        description="Log to file"
    )

    class Config:
        env_prefix = "LOG_"

class OpcUaSettings(BaseSettings):
    """OPC UA connection and behavior settings"""
    url: str = Field(
        default="opc.tcp://localhost:4840/", 
        description="OPC UA server URL"
    )
    connection_timeout: float = Field(
        default=10.0, 
        description="Connection timeout in seconds"
    )
    reconnect_delay: float = Field(
        default=5.0, 
        description="Delay between reconnection attempts in seconds"
    )
    max_reconnect_attempts: int = Field(
        default=3, 
        description="Maximum number of reconnection attempts"
    )
    connection_check_interval: int = Field(
        default=30, 
        description="Interval in seconds for checking connection health"
    )

    class Config:
        env_prefix = "OPC_UA_"

class KafkaSettings(BaseSettings):
    """Kafka configuration settings"""
    enabled: bool = Field(
        default=False,
        description="Enable Kafka integration"
    )
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers"
    )
    default_topic: str = Field(
        default="plant_data",
        description="Default Kafka topic for plant data"
    )
    producer_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional Kafka producer configuration"
    )

    class Config:
        env_prefix = "KAFKA_"

class Settings(BaseSettings):
    """Main application settings"""
    app_name: str = Field(
        default="Data Ingestion Microservice",
        description="Application name"
    )
    debug: bool = Field(
        default=False,
        description="Debug mode"
    )
    environment: str = Field(
        default="development",
        description="Environment (development, testing, production)"
    )
    api_version: str = Field(
        default="v1",
        description="API version"
    )
    
    # Nested settings
    db: DatabaseSettings = DatabaseSettings()
    redis: RedisSettings = RedisSettings()
    jwt: JWTSettings = JWTSettings()
    logging: LoggingSettings = LoggingSettings()
    opcua: OpcUaSettings = OpcUaSettings()
    kafka: KafkaSettings = KafkaSettings()

    class Config:
        env_prefix = "APP_"

    @property
    def CENTRAL_DATABASE_URL(self) -> str:
        """Get central database URL - for backward compatibility"""
        return self.db.url

    def get_plant_database_url(self, database_key: str) -> str:
        """Get database URL for a specific plant using its database key
        
        Args:
            database_key: The database key from plants_registry (e.g., 'CAIRO_DB', 'ALEX_DB')
            
        Returns:
            Database URL for the plant
            
        Raises:
            ValueError: If required environment variables are missing
        """
        # First try to get plant-specific database configuration
        db_user = os.getenv(f"{database_key}_USER")
        db_password = os.getenv(f"{database_key}_PASSWORD")
        db_host = os.getenv(f"{database_key}_HOST")
        db_port = os.getenv(f"{database_key}_PORT", "5432")
        db_name = os.getenv(f"{database_key}_NAME")
        
        # If plant-specific configuration is not found, fall back to default PLANT_DATABASE configuration
        if not all([db_user, db_password, db_host, db_port, db_name]):
            logger.warning(f"Plant-specific database configuration for {database_key} not found, falling back to PLANT_DATABASE")
            
            # Try to get default plant database configuration
            db_user = os.getenv("PLANT_DATABASE_USER")
            db_password = os.getenv("PLANT_DATABASE_PASSWORD")
            db_host = os.getenv("PLANT_DATABASE_HOST")
            db_port = os.getenv("PLANT_DATABASE_PORT", "5432")
            db_name = os.getenv("PLANT_DATABASE_NAME")
        
        if not all([db_user, db_password, db_host, db_port, db_name]):
            raise ValueError(f"Missing required environment variables for plant database: {database_key} and fallback PLANT_DATABASE configuration")
        
        return f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create and export settings instance
settings = Settings()

def get_settings() -> Settings:
    """Get the application settings

    Returns:
        Settings: The application settings
    """
    return settings

def get_database_config() -> dict:
    """Get database configuration for health checks and monitoring
    
    Returns:
        Dictionary with database configuration info
    """
    return {
        "central_db": {
            "host": settings.db.host,
            "port": settings.db.port,
            "name": settings.db.name,
            "pool_size": settings.db.pool_size,
            "max_overflow": settings.db.max_overflow
        },
        "redis": {
            "host": settings.redis.host,
            "port": settings.redis.port,
            "db": settings.redis.db
        }
    }

def validate_plant_database_config(database_key: str) -> bool:
    """Validate that a plant database configuration exists
    
    Args:
        database_key: The database key to validate
        
    Returns:
        True if configuration exists, False otherwise
    """
    required_vars = [
        f"{database_key}_USER",
        f"{database_key}_PASSWORD", 
        f"{database_key}_HOST",
        f"{database_key}_NAME"
    ]
    
    return all(os.getenv(var) is not None for var in required_vars) 
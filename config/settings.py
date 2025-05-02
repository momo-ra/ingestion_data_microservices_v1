"""
Application Configuration

This module centralizes all configuration settings for the application.
It loads configuration from environment variables with sensible defaults.
"""

import os
from typing import Dict, Any, Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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

class DatabaseSettings(BaseSettings):
    """Database connection settings"""
    url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/opcua_db",
        description="Database connection URL"
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
        default="opcua_data",
        description="Default Kafka topic for OPC UA data"
    )

    class Config:
        env_prefix = "KAFKA_"

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

class Settings(BaseSettings):
    """Main application settings"""
    app_name: str = Field(
        default="OPC UA Data Ingestion Service",
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
    default_polling_interval: int = Field(
        default=60,
        description="Default polling interval in seconds"
    )
    
    # Nested settings
    opcua: OpcUaSettings = OpcUaSettings()
    db: DatabaseSettings = DatabaseSettings()
    kafka: KafkaSettings = KafkaSettings()
    logging: LoggingSettings = LoggingSettings()

    class Config:
        env_prefix = "APP_"

# Create and export settings instance
settings = Settings()

def get_settings() -> Settings:
    """Get the application settings

    Returns:
        Settings: The application settings
    """
    return settings 
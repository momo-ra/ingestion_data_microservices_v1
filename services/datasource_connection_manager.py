"""
DataSource Connection Manager

This service manages connections to various data sources using the datasources library.
It provides a unified interface for connecting to OPC UA, databases, and other data sources.
"""

import asyncio
from typing import Dict, Any, Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from utils.log import setup_logger
from utils.singleton import Singleton
from utils.error_handling import handle_async_errors, ConnectionError
from utils.metrics import async_time_metric
from queries.datasource_queries import get_data_source_by_id
from models.plant_models import DataSource

# Import datasources library
try:
    from datasources_lib import ConnectionPool, OpcUaConfig, DatabaseConfig
    DATASOURCES_AVAILABLE = True
except ImportError:
    DATASOURCES_AVAILABLE = False
    logger = setup_logger(__name__)
    logger.warning("datasources-lib not available. Install it to use datasource connections.")

logger = setup_logger(__name__)

class DataSourceConnectionManager(Singleton):
    """Manages connections to various data sources using the datasources library with proper connection pooling"""
    
    def __init__(self):
        """Initialize the connection manager"""
        # Avoid re-initialization if already initialized
        if hasattr(self, 'initialized') and self.initialized:
            return
            
        if not DATASOURCES_AVAILABLE:
            logger.error("datasources-lib is not available. Cannot initialize connection manager.")
            self.initialized = False
            return
            
        self._pool = None
        self._config_cache = {}  # Cache for datasource configurations
        self._lock = asyncio.Lock()
        self.initialized = True
        
    @handle_async_errors(error_class=ConnectionError, default_message="Failed to start connection pool")
    @async_time_metric("datasource_operation_duration", {"operation": "start_pool"})
    async def start(self) -> bool:
        """Start the connection pool (singleton pattern)"""
        if not DATASOURCES_AVAILABLE:
            raise ConnectionError("datasources-lib is not available")
            
        async with self._lock:
            if self._pool is None:
                self._pool = ConnectionPool()
                await self._pool.start()
                logger.info("DataSource connection pool started successfully")
            return True
    
    @handle_async_errors(error_class=ConnectionError, default_message="Failed to stop connection pool")
    @async_time_metric("datasource_operation_duration", {"operation": "stop_pool"})
    async def stop(self) -> bool:
        """Stop the connection pool"""
        if not DATASOURCES_AVAILABLE:
            return True
            
        async with self._lock:
            if self._pool:
                await self._pool.stop()
                self._pool = None
                logger.info("DataSource connection pool stopped successfully")
            return True
    
    async def get_connection_pool(self):
        """Get or create connection pool (singleton pattern)"""
        if not self._pool:
            await self.start()
        return self._pool
    
    @handle_async_errors(error_class=ConnectionError, default_message="Failed to get datasource configuration")
    async def get_datasource_config(self, session: AsyncSession, source_id: int, plant_id: int) -> Any:
        """Convert database record to library config object"""
        if not DATASOURCES_AVAILABLE:
            raise ConnectionError("datasources-lib is not available")
            
        # Check cache first
        cache_key = f"{plant_id}_{source_id}"
        if cache_key in self._config_cache:
            return self._config_cache[cache_key]
        
        # Get datasource from database
        datasource = await get_data_source_by_id(session, source_id, plant_id)
        if not datasource:
            raise ConnectionError(f"DataSource {source_id} not found in plant {plant_id}")
        
        if not datasource.is_active:
            raise ConnectionError(f"DataSource {source_id} is not active")
        
        # Convert to library configuration
        config = self._convert_to_library_config(datasource)
        
        # Cache the configuration
        self._config_cache[cache_key] = config
        
        return config
    
    def _convert_to_library_config(self, datasource: DataSource) -> Any:
        """Convert database datasource to library configuration"""
        if not DATASOURCES_AVAILABLE:
            raise ConnectionError("datasources-lib is not available")
            
        datasource_type = datasource.data_source_type.name.lower()
        connection_config = datasource.connection_config or {}
        
        # Add name and type to config
        config_data = connection_config.copy()
        config_data["name"] = datasource.name
        
        # Map datasource types to library types
        if datasource_type in ["opcua", "opc-ua", "opc_ua", "opc ua"]:
            config_data["type"] = "opcua"
        elif datasource_type in ["database", "postgresql", "mysql", "sqlite"]:
            config_data["type"] = "database"
        else:
            config_data["type"] = datasource_type
        
        if datasource_type in ["opcua", "opc-ua", "opc_ua", "opc ua"]:
            # Ensure required fields with defaults
            config_data.setdefault("test_node_id", None)  # Important: disable test node
            config_data.setdefault("connection_timeout", 30.0)
            config_data.setdefault("max_retries", 3)
            
            return OpcUaConfig(**config_data)
            
        elif datasource_type in ["database", "postgresql", "mysql", "sqlite"]:
            # Ensure required fields with defaults
            config_data.setdefault("port", 5432)
            config_data.setdefault("db_type", "postgresql" if datasource_type == "postgresql" else datasource_type)
            
            return DatabaseConfig(**config_data)
        else:
            raise ConnectionError(f"Unsupported datasource type: {datasource_type}")
    
    def _validate_connection_config(self, datasource: DataSource) -> bool:
        """Validate connection configuration before attempting connection"""
        try:
            datasource_type = datasource.data_source_type.name.lower()
            connection_config = datasource.connection_config or {}
            
            if datasource_type in ["database", "postgresql", "mysql", "sqlite"]:
                # Validate required fields for database connections
                required_fields = ["host", "database", "username", "password"]
                for field in required_fields:
                    if field not in connection_config or not connection_config[field]:
                        logger.error(f"Missing required field '{field}' for database connection")
                        return False
                
                # Validate host format
                host = connection_config.get("host", "")
                if not host or len(host.strip()) < 3:  # Basic host validation
                    logger.error(f"Invalid host format: '{host}'")
                    return False
                
                # Validate port
                port = connection_config.get("port", 5432)
                if not isinstance(port, int) or port < 1 or port > 65535:
                    logger.error(f"Invalid port: {port}")
                    return False
                
                logger.info(f"Connection configuration validation passed for {datasource.name}")
                return True
                
            elif datasource_type in ["opcua", "opc-ua", "opc_ua", "opc ua"]:
                # Validate required fields for OPC UA connections
                required_fields = ["url"]
                for field in required_fields:
                    if field not in connection_config or not connection_config[field]:
                        logger.error(f"Missing required field '{field}' for OPC UA connection")
                        return False
                
                # Validate URL format
                url = connection_config.get("url", "")
                if not url.startswith("opc.tcp://"):
                    logger.error(f"Invalid OPC UA URL format: '{url}'")
                    return False
                
                logger.info(f"Connection configuration validation passed for {datasource.name}")
                return True
            
            return True  # For other types, assume valid
            
        except Exception as e:
            logger.error(f"Error validating connection configuration: {e}")
            return False
    
    @handle_async_errors(error_class=ConnectionError, default_message="Failed to get connection")
    @async_time_metric("datasource_operation_duration", {"operation": "get_connection"})
    async def get_connection(self, session: AsyncSession, source_id: int, plant_id: int):
        """Get a connection to a specific datasource"""
        if not DATASOURCES_AVAILABLE:
            raise ConnectionError("datasources-lib is not available")
        
        config = await self.get_datasource_config(session, source_id, plant_id)
        pool = await self.get_connection_pool()
        return pool.get_connection(config)
    
    @handle_async_errors(error_class=ConnectionError, default_message="Failed to test connection")
    @async_time_metric("datasource_operation_duration", {"operation": "test_connection"})
    async def test_connection(self, session: AsyncSession, source_id: int, plant_id: int) -> Dict[str, Any]:
        """Test connection to a specific datasource"""
        if not DATASOURCES_AVAILABLE:
            return {"success": False, "error": "datasources-lib is not available"}
            
        try:
            async with await self.get_connection(session, source_id, plant_id) as conn:
                is_healthy, error = await conn.test_connection()
                return {
                    "success": is_healthy,
                    "message": "Connection successful" if is_healthy else f"Connection failed: {error}",
                    "error": error if not is_healthy else None
                }
        except Exception as e:
            logger.error(f"Error testing connection to datasource {source_id} in plant {plant_id}: {e}")
            return {"success": False, "error": str(e)}
    
    @handle_async_errors(error_class=ConnectionError, default_message="Failed to test connection")
    @async_time_metric("datasource_operation_duration", {"operation": "test_connection_object"})
    async def test_connection_object(self, datasource: DataSource) -> bool:
        """Test connection to a datasource using DataSource object directly"""
        if not DATASOURCES_AVAILABLE:
            logger.error("datasources-lib is not available")
            return False
            
        try:
            # Validate connection configuration
            if not self._validate_connection_config(datasource):
                return False
            
            # Convert datasource to library config
            config = self._convert_to_library_config(datasource)
            
            # Get connection pool
            pool = await self.get_connection_pool()
            
            # Get connection and test it
            async with pool.get_connection(config) as conn:
                is_healthy, error = await conn.test_connection()
                
                if is_healthy:
                    # Additional validation for database connections
                    if datasource.data_source_type.name.lower() in ["database", "postgresql", "mysql", "sqlite"]:
                        # Try to execute a simple query to verify connection
                        try:
                            if hasattr(conn, 'supports_queries') and conn.supports_queries:
                                # Execute a simple query like "SELECT 1"
                                result = await conn.query("SELECT 1", {})
                                if not result:
                                    logger.error(f"Database connection test failed - query returned no result for {datasource.name}")
                                    return False
                            else:
                                # For non-query datasources, just log the test
                                logger.info(f"Database connection test passed for {datasource.name}")
                        except Exception as query_error:
                            logger.error(f"Database connection test failed - query error for {datasource.name}: {query_error}")
                            return False
                    
                    logger.info(f"Successfully tested connection for datasource {datasource.name}")
                    return True
                else:
                    logger.error(f"Connection test failed for datasource {datasource.name}: {error}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error testing connection for datasource {datasource.name}: {e}")
            return False
    
    @handle_async_errors(error_class=ConnectionError, default_message="Failed to read node")
    @async_time_metric("datasource_operation_duration", {"operation": "read_node"})
    async def read_node(self, session: AsyncSession, source_id: int, plant_id: int, node_id: str) -> Dict[str, Any]:
        """Read a node from a datasource"""
        if not DATASOURCES_AVAILABLE:
            raise ConnectionError("datasources-lib is not available")
            
        try:
            async with await self.get_connection(session, source_id, plant_id) as conn:
                result = await conn.read_node(node_id)
                
                if result:
                    return {
                        "success": True,
                        "node_id": node_id,
                        "value": result.value,
                        "timestamp": result.timestamp.isoformat() if result.timestamp else None,
                        "quality": result.quality
                    }
                else:
                    return {
                        "success": False,
                        "node_id": node_id,
                        "error": "Failed to read node - node not found or not accessible"
                    }
                    
        except Exception as e:
            logger.error(f"Error reading node {node_id} from datasource {source_id} in plant {plant_id}: {e}")
            return {
                "success": False,
                "node_id": node_id,
                "error": str(e)
            }
    
    @handle_async_errors(error_class=ConnectionError, default_message="Failed to read nodes")
    @async_time_metric("datasource_operation_duration", {"operation": "read_nodes"})
    async def read_nodes(self, session: AsyncSession, source_id: int, plant_id: int, node_ids: List[str]) -> Dict[str, Any]:
        """Read multiple nodes from a datasource"""
        if not DATASOURCES_AVAILABLE:
            raise ConnectionError("datasources-lib is not available")
            
        try:
            async with await self.get_connection(session, source_id, plant_id) as conn:
                results = await conn.read_nodes(node_ids)
                
                # Convert to API response format
                response_data = []
                for node_id in node_ids:
                    node_value = results.get(node_id)
                    if node_value:
                        response_data.append({
                            "node_id": node_id,
                            "value": node_value.value,
                            "timestamp": node_value.timestamp.isoformat() if node_value.timestamp else None,
                            "quality": node_value.quality
                        })
                    else:
                        response_data.append({
                            "node_id": node_id,
                            "value": None,
                            "timestamp": None,
                            "quality": "BAD",
                            "error": "Failed to read node"
                        })
                
                return {"success": True, "data": response_data}
                
        except Exception as e:
            logger.error(f"Error reading nodes {node_ids} from datasource {source_id} in plant {plant_id}: {e}")
            return {"success": False, "error": str(e)}
    
    @handle_async_errors(error_class=ConnectionError, default_message="Failed to write node")
    @async_time_metric("datasource_operation_duration", {"operation": "write_node"})
    async def write_node(self, session: AsyncSession, source_id: int, plant_id: int, node_id: str, value: Any) -> Dict[str, Any]:
        """Write a value to a node in a datasource"""
        if not DATASOURCES_AVAILABLE:
            raise ConnectionError("datasources-lib is not available")
            
        try:
            async with await self.get_connection(session, source_id, plant_id) as conn:
                success = await conn.write_node(node_id, value)
                return {
                    "success": success,
                    "node_id": node_id,
                    "value": value,
                    "message": "Write successful" if success else "Write failed"
                }
                
        except Exception as e:
            logger.error(f"Error writing to node {node_id} in datasource {source_id} for plant {plant_id}: {e}")
            return {
                "success": False,
                "node_id": node_id,
                "value": value,
                "error": str(e)
            }
    
    @handle_async_errors(error_class=ConnectionError, default_message="Failed to query database")
    @async_time_metric("datasource_operation_duration", {"operation": "query"})
    async def query(self, session: AsyncSession, source_id: int, plant_id: int, sql: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute a query on a database datasource"""
        if not DATASOURCES_AVAILABLE:
            raise ConnectionError("datasources-lib is not available")
            
        try:
            async with await self.get_connection(session, source_id, plant_id) as conn:
                if hasattr(conn, 'supports_queries') and conn.supports_queries:
                    result = await conn.query(sql, params or {})
                    return {"success": True, "data": result}
                else:
                    return {"success": False, "error": "This datasource doesn't support SQL queries"}
                    
        except Exception as e:
            logger.error(f"Error executing query on datasource {source_id} in plant {plant_id}: {e}")
            return {"success": False, "error": str(e)}
    
    async def clear_cache(self):
        """Clear the configuration cache"""
        self._config_cache.clear()
        logger.info("DataSource configuration cache cleared")
    
    async def shutdown(self):
        """Cleanup connection pool"""
        await self.stop()

def get_datasource_connection_manager():
    """Get the singleton instance of DataSourceConnectionManager"""
    return DataSourceConnectionManager.get_instance() 
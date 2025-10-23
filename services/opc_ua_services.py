import os
from dotenv import load_dotenv
import asyncio
from asyncua import Client
from typing import Dict, Any, Optional, List, Tuple
from utils.log import setup_logger
from utils.error_handling import handle_async_errors, ConnectionError
from utils.metrics import async_time_metric, opcua_connection_attempts, opcua_requests_total, opcua_request_duration, opcua_connection_status
from config.settings import settings
from utils.singleton import Singleton

load_dotenv("./../.env", override=True)
logger = setup_logger(__name__)

def get_opc_ua_client():
    """Get the singleton instance of OpcUaClient"""
    return OpcUaClient.get_instance()

class OpcUaClient(Singleton):
    """OPC UA client with connection management and retries"""
    
    def __init__(self):
        """Initialize the OPC UA client with settings from configuration"""
        # Avoid re-initialization if already initialized
        if hasattr(self, 'initialized') and self.initialized:
            return
            
        self.url = settings.opcua.url
        logger.info(f"Using OPC-UA server URL: {self.url}")
        
        # Initialize client properties
        self.client = Client(self.url)
        self.connected = False
        self._reconnect_lock = asyncio.Lock()
        
        # Connection monitor properties
        self.connection_monitor_task = None
        self.connection_monitor_running = False
        self.connection_check_interval = settings.opcua.connection_check_interval
        
        # Polling and subscription tracking
        self.polling_tasks = {}
        self.subscription_handles = {}
        
        # Set initial connection status metric
        opcua_connection_status.set(0, {"url": self.url})
        
        # Mark as initialized
        self.initialized = True
        
    @handle_async_errors(error_class=ConnectionError, default_message="Failed to connect to OPC-UA server")
    @async_time_metric("opcua_operation_duration", {"operation": "connect"})
    async def connect(self) -> bool:
        """Connect to the OPC-UA server with retry logic

        Returns:
            bool: True if successfully connected, False otherwise
        
        Raises:
            ConnectionError: If connection fails after retries
        """
        async with self._reconnect_lock:
            if self.connected:
                return True
                
            # Create a new client instance
            self.client = Client(self.url)
            
            # Connect with retry logic
            max_retries = settings.opcua.max_reconnect_attempts
            retry_count = 0
            
            # Track connection attempt
            opcua_connection_attempts.inc(1, {"url": self.url, "success": "unknown"})
            
            while retry_count < max_retries:
                try:
                    await self.client.connect()
                    self.connected = True
                    logger.info(f"Connected to OPC-UA server at {self.url}", 
                               extra={"structured_data": {"url": self.url}})
                    
                    # Update connection metrics
                    opcua_connection_status.set(1, {"url": self.url})
                    opcua_connection_attempts.inc(1, {"url": self.url, "success": "true"})
                    
                    # Start connection monitor if not already running
                    await self.start_connection_monitor()
                    
                    return True
                except Exception as conn_error:
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"Connection attempt {retry_count} failed: {conn_error}. Retrying...",
                                     extra={"structured_data": {"retry_count": retry_count, "error": str(conn_error)}})
                        await asyncio.sleep(settings.opcua.reconnect_delay)
                    else:
                        logger.error(f"Failed to connect after {max_retries} attempts: {conn_error}",
                                   extra={"structured_data": {"max_retries": max_retries, "error": str(conn_error)}})
                        
                        # Update connection metrics
                        opcua_connection_status.set(0, {"url": self.url})
                        opcua_connection_attempts.inc(1, {"url": self.url, "success": "false"})
                        
                        raise ConnectionError(
                            message=f"Failed to connect to OPC-UA server at {self.url} after {max_retries} attempts",
                            error_code="CONNECTION_FAILED",
                            details={"url": self.url, "max_retries": max_retries}
                        )
            
            return False

    @handle_async_errors(error_class=ConnectionError, default_message="Error during OPC-UA disconnection")
    @async_time_metric("opcua_operation_duration", {"operation": "disconnect"})
    async def disconnect(self) -> bool:
        """Disconnect from the OPC-UA server and clean up resources

        Returns:
            bool: True if successfully disconnected, False otherwise
        
        Raises:
            ConnectionError: If error occurs during disconnection
        """
        if self.client and self.connected:
            try:
                # Track request
                opcua_requests_total.inc(1, {"operation": "disconnect", "success": "unknown"})
                
                # Stop the connection monitor
                await self.stop_connection_monitor()
                
                # Disconnect client
                await self.client.disconnect()
                self.connected = False
                
                # Update connection metrics
                opcua_connection_status.set(0, {"url": self.url})
                opcua_requests_total.inc(1, {"operation": "disconnect", "success": "true"})
                
                logger.info("Disconnected from OPC-UA server")
                return True
            except Exception as e:
                logger.error(f"Error during disconnection: {e}",
                           extra={"structured_data": {"error": str(e)}})
                           
                # Update metrics
                opcua_requests_total.inc(1, {"operation": "disconnect", "success": "false"})
                
                raise ConnectionError(
                    message=f"Error during disconnection: {e}",
                    error_code="DISCONNECT_ERROR",
                    details={"error": str(e)}
                )
        return True

    @handle_async_errors(error_class=ConnectionError, default_message="Error starting connection monitor")
    async def start_connection_monitor(self) -> bool:
        """Start a background task to monitor connection status

        Returns:
            bool: True if monitor started successfully
        
        Raises:
            ConnectionError: If error occurs starting monitor
        """
        if self.connection_monitor_running:
            logger.info("Connection monitor is already running")
            return True
        
        from utils.task_manager import TaskManager
        
        task_manager = TaskManager.get_instance()
        self.connection_monitor_running = True
        
        # Start the connection monitor task using the task manager
        success = await task_manager.start_task(
            name="opcua_connection_monitor",
            coro=self._connection_monitor_worker(),
            restart_on_failure=True
        )
        
        if success:
            logger.info("Started connection monitor task")
        
        return success

    @handle_async_errors(error_class=ConnectionError, default_message="Error stopping connection monitor")
    async def stop_connection_monitor(self) -> bool:
        """Stop the connection monitor task

        Returns:
            bool: True if monitor stopped successfully
        
        Raises:
            ConnectionError: If error occurs stopping monitor
        """
        if not self.connection_monitor_running:
            return True
            
        from utils.task_manager import TaskManager
        
        task_manager = TaskManager.get_instance()
        self.connection_monitor_running = False
        
        # Stop the connection monitor task using the task manager
        success = await task_manager.stop_task("opcua_connection_monitor")
        
        if success:
            logger.info("Stopped connection monitor task")
            
        return success

    async def _connection_monitor_worker(self) -> None:
        """Worker function for connection monitoring"""
        try:
            while self.connection_monitor_running:
                try:
                    # Check if we're connected
                    if not self.connected:
                        logger.info("Connection monitor detected disconnection, attempting to reconnect...")
                        await self.connect()
                    else:
                        # Perform a simple operation to verify connection is still alive
                        try:
                            # Try to read a simple node to verify connection
                            node = self.client.get_node("i=2258")  # Server status node
                            await node.read_browse_name()
                            logger.debug("Connection is healthy")
                        except Exception as e:
                            logger.warning(f"Connection check failed: {e}",
                                         extra={"structured_data": {"error": str(e)}})
                            self.connected = False
                            
                            # Update connection status metric
                            opcua_connection_status.set(0, {"url": self.url})
                            
                            # Try to reconnect
                            logger.info("Attempting to reconnect...")
                            await self.connect()
                except Exception as e:
                    logger.error(f"Error in connection monitor: {e}",
                               extra={"structured_data": {"error": str(e)}})
                
                # Wait before next check
                await asyncio.sleep(self.connection_check_interval)
        except asyncio.CancelledError:
            logger.info("Connection monitor task cancelled")
        except Exception as e:
            logger.error(f"Unexpected error in connection monitor: {e}",
                       extra={"structured_data": {"error": str(e)}})
            import traceback
            logger.error(traceback.format_exc())
            # Restart the monitor
            await asyncio.sleep(5)
            await self.start_connection_monitor()
            
    @handle_async_errors(default_message="Error getting node value")
    @async_time_metric("opcua_operation_duration", {"operation": "read_node"})
    async def get_value_of_specific_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get value and metadata of a specific OPC UA node

        Args:
            node_id: The OPC UA node ID

        Returns:
            Dict with node data or None if error
        """
        # Track request
        opcua_requests_total.inc(1, {"operation": "read_node", "success": "unknown"})
        
        try:
            if not self.connected:
                success = await self.connect()
                if not success:
                    opcua_requests_total.inc(1, {"operation": "read_node", "success": "false"})
                    return None
                
            node = self.client.get_node(node_id)
            value = await node.get_value()
            data_value = await node.read_data_value()
            timestamp = data_value.SourceTimestamp.isoformat() if data_value.SourceTimestamp else None
            status = data_value.StatusCode

            logger.info(f"Read node {node_id}",
                       extra={"structured_data": {
                           "node_id": node_id,
                           "value": value,
                           "timestamp": timestamp,
                           "status": str(status)
                       }})
                       
            # Track successful request
            opcua_requests_total.inc(1, {"operation": "read_node", "success": "true"})

            return {
                "node_id": node_id,
                "value": value,
                "timestamp": timestamp,
                "status": str(status)
            }
        except Exception as e:
            # Track failed request
            opcua_requests_total.inc(1, {"operation": "read_node", "success": "false"})
            raise


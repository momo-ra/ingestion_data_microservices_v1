"""
Monitoring Service

This module provides functionality for monitoring the health of OPC-UA servers
and other system components.
"""

import asyncio
import time
from datetime import datetime, timedelta
from utils.log import setup_logger
from services.opc_ua_services import get_opc_ua_client

logger = setup_logger(__name__)

class MonitoringService:
    """Service for monitoring system health and components"""
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get the singleton instance of MonitoringService"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """Initialize the monitoring service"""
        self.monitoring_task = None
        self.monitoring_running = False
        self.check_interval = 60  # seconds
        self.health_status = {
            "opc_ua_server": {
                "status": "unknown",
                "last_check": None,
                "details": None,
                "metrics": {}
            },
            "database": {
                "status": "unknown",
                "last_check": None,
                "details": None,
                "metrics": {}
            },
            "time_series_data": {
                "status": "unknown",
                "last_check": None,
                "details": None,
                "metrics": {}
            }
        }
        
    async def start_monitoring(self):
        """Start the monitoring task"""
        if self.monitoring_running:
            logger.info("Monitoring is already running")
            return
        
        self.monitoring_running = True
        self.monitoring_task = asyncio.create_task(self._monitoring_worker())
        logger.info("Started monitoring task")
        
    async def stop_monitoring(self):
        """Stop the monitoring task"""
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_running = False
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
            logger.info("Stopped monitoring task")
            
    async def _monitoring_worker(self):
        """Worker function for monitoring system health"""
        try:
            while self.monitoring_running:
                try:
                    # Check OPC-UA server health
                    await self._check_opcua_health()
                    
                    # Check database health
                    await self._check_database_health()
                    
                    # Check time series data health
                    await self._check_time_series_health()
                    
                    # Log overall system health
                    self._log_system_health()
                except Exception as e:
                    logger.error(f"Error in monitoring worker: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                
                # Wait before next check
                await asyncio.sleep(self.check_interval)
        except asyncio.CancelledError:
            logger.info("Monitoring task cancelled")
        except Exception as e:
            logger.error(f"Unexpected error in monitoring worker: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Restart the monitor
            await asyncio.sleep(5)
            await self.start_monitoring()
            
    async def _check_opcua_health(self):
        """Check OPC-UA server health"""
        try:
            client = get_opc_ua_client()
            now = datetime.now()
            
            if client.connected:
                # Try to read a simple node to verify connection is still alive
                try:
                    # Try to read a simple node to verify connection
                    node = client.client.get_node("i=2258")  # Server status node
                    await node.read_browse_name()
                    
                    # Get additional metrics
                    metrics = {
                        "active_polling_tasks": len(client.polling_tasks),
                        "active_subscriptions": len(client.subscription_handles),
                        "connection_uptime": time.time() - client.polling_tasks.get(list(client.polling_tasks.keys())[0], {}).get("last_poll", time.time()) if client.polling_tasks else 0
                    }
                    
                    self.health_status["opc_ua_server"] = {
                        "status": "healthy",
                        "last_check": now,
                        "details": "Connected and responsive",
                        "metrics": metrics
                    }
                    logger.debug("OPC-UA server is healthy")
                except Exception as e:
                    self.health_status["opc_ua_server"] = {
                        "status": "degraded",
                        "last_check": now,
                        "details": f"Connected but not responsive: {str(e)}",
                        "metrics": {}
                    }
                    logger.warning(f"OPC-UA server is degraded: {e}")
            else:
                self.health_status["opc_ua_server"] = {
                    "status": "unhealthy",
                    "last_check": now,
                    "details": "Not connected",
                    "metrics": {}
                }
                logger.warning("OPC-UA server is unhealthy: Not connected")
        except Exception as e:
            self.health_status["opc_ua_server"] = {
                "status": "error",
                "last_check": datetime.now(),
                "details": str(e),
                "metrics": {}
            }
            logger.error(f"Error checking OPC-UA server health: {e}")
            
    async def _check_database_health(self):
        """Check database health"""
        try:
            from database import check_db_connection
            now = datetime.now()
            
            is_healthy = await check_db_connection()
            
            if is_healthy:
                # Get additional database metrics
                try:
                    from sqlalchemy import text
                    from database import async_session
                    
                    metrics = {}
                    
                    # Check connection count
                    async with async_session() as session:
                        result = await session.execute(text("SELECT count(*) FROM pg_stat_activity"))
                        metrics["active_connections"] = result.scalar()
                        
                        # Check database size
                        result = await session.execute(text("SELECT pg_database_size(current_database())"))
                        metrics["database_size_bytes"] = result.scalar()
                        
                        # Check table counts
                        result = await session.execute(text("SELECT count(*) FROM time_series"))
                        metrics["time_series_count"] = result.scalar()
                        
                        result = await session.execute(text("SELECT count(*) FROM tag"))
                        metrics["tag_count"] = result.scalar()
                        
                        result = await session.execute(text("SELECT count(*) FROM polling_tasks WHERE is_active = true"))
                        metrics["active_polling_tasks"] = result.scalar()
                except Exception as metrics_error:
                    logger.warning(f"Error getting database metrics: {metrics_error}")
                    metrics = {}
                
                self.health_status["database"] = {
                    "status": "healthy",
                    "last_check": now,
                    "details": "Connected and responsive",
                    "metrics": metrics
                }
                logger.debug("Database is healthy")
            else:
                self.health_status["database"] = {
                    "status": "unhealthy",
                    "last_check": now,
                    "details": "Not connected or not responsive",
                    "metrics": {}
                }
                logger.warning("Database is unhealthy")
        except Exception as e:
            self.health_status["database"] = {
                "status": "error",
                "last_check": datetime.now(),
                "details": str(e),
                "metrics": {}
            }
            logger.error(f"Error checking database health: {e}")
            
    async def _check_time_series_health(self):
        """Check time series data health"""
        try:
            now = datetime.now()
            
            # Check if we have recent time series data
            try:
                from sqlalchemy import text, func
                from database import async_session
                from models.models import TimeSeries
                from sqlalchemy import select
                
                metrics = {}
                
                # Get the most recent timestamp
                async with async_session() as session:
                    query = select(func.max(TimeSeries.timestamp)).select_from(TimeSeries)
                    result = await session.execute(query)
                    latest_timestamp = result.scalar()
                    
                    if latest_timestamp:
                        # Calculate time since last data point
                        time_since_last = now - latest_timestamp
                        metrics["time_since_last_data"] = time_since_last.total_seconds()
                        metrics["latest_timestamp"] = latest_timestamp.isoformat()
                        
                        # Get data points in the last hour
                        one_hour_ago = now - timedelta(hours=1)
                        query = select(func.count()).select_from(TimeSeries).where(TimeSeries.timestamp >= one_hour_ago)
                        result = await session.execute(query)
                        metrics["data_points_last_hour"] = result.scalar()
                        
                        # Determine status based on recency
                        if time_since_last.total_seconds() < 300:  # Less than 5 minutes
                            status = "healthy"
                            details = "Recent time series data available"
                        elif time_since_last.total_seconds() < 3600:  # Less than 1 hour
                            status = "degraded"
                            details = f"Time series data is {time_since_last.total_seconds()/60:.1f} minutes old"
                        else:
                            status = "unhealthy"
                            details = f"Time series data is {time_since_last.total_seconds()/3600:.1f} hours old"
                    else:
                        status = "unhealthy"
                        details = "No time series data available"
                        metrics = {}
                
                self.health_status["time_series_data"] = {
                    "status": status,
                    "last_check": now,
                    "details": details,
                    "metrics": metrics
                }
                logger.debug(f"Time series data health: {status}")
            except Exception as metrics_error:
                logger.warning(f"Error checking time series data health: {metrics_error}")
                self.health_status["time_series_data"] = {
                    "status": "unknown",
                    "last_check": now,
                    "details": f"Error checking time series data: {str(metrics_error)}",
                    "metrics": {}
                }
        except Exception as e:
            self.health_status["time_series_data"] = {
                "status": "error",
                "last_check": datetime.now(),
                "details": str(e),
                "metrics": {}
            }
            logger.error(f"Error checking time series data health: {e}")
            
    def _log_system_health(self):
        """Log overall system health"""
        try:
            # Determine overall system health
            statuses = [component["status"] for component in self.health_status.values()]
            
            if "error" in statuses or "unhealthy" in statuses:
                overall_status = "unhealthy"
            elif "degraded" in statuses:
                overall_status = "degraded"
            else:
                overall_status = "healthy"
                
            logger.info(f"System health: {overall_status}")
            for component, status in self.health_status.items():
                logger.debug(f"  {component}: {status['status']} - {status['details']}")
        except Exception as e:
            logger.error(f"Error logging system health: {e}")
            
    def get_health_status(self):
        """Get the current health status
        
        Returns:
            dict: The current health status
        """
        # Add overall status
        statuses = [component["status"] for component in self.health_status.values()]
        
        if "error" in statuses or "unhealthy" in statuses:
            overall_status = "unhealthy"
        elif "degraded" in statuses:
            overall_status = "degraded"
        else:
            overall_status = "healthy"
            
        return {
            "status": overall_status,
            "timestamp": datetime.now().isoformat(),
            "components": self.health_status
        } 
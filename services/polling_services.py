import asyncio
import time
import threading
from utils.log import setup_logger
from services.scheduler_services import SchedulerService
from queries.polling_queries import save_polling_task, deactivate_polling_task, update_polling_task_timestamp, get_active_polling_tasks
from queries.timeseries_queries import save_plant_node_data_to_db
from queries.tag_queries import validate_opcua_connection_string
from services.kafka_services import kafka_service
from services.opc_ua_services import get_opc_ua_client
from database import get_plant_db
from utils.singleton import Singleton
from utils.error_handling import handle_async_errors
from tenacity import retry, stop_after_attempt, wait_fixed
import re

logger = setup_logger(__name__)

# Singleton instance
_polling_service_instance = None
_polling_service_lock = threading.Lock()

def get_polling_service():
    """Get the singleton instance of PollingService"""
    return PollingService.get_instance()

class PollingService(Singleton):
    """Polling service with database integration and job scheduling"""
    
    @classmethod
    def get_instance(cls):
        """Get the singleton instance"""
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """Initialize the polling service"""
        # Avoid re-initialization if already initialized
        if hasattr(self, 'initialized') and self.initialized:
            return
            
        self.polling_tasks = {}  # Dictionary to store polling task information: {plant_id: {node_id: task_info}}
        
        # Get scheduler instance
        self.scheduler = SchedulerService.get_instance()
        
        # Get OPC UA client
        self.opc_client = get_opc_ua_client()
        
        # Default plant for now - should be configurable
        self.default_plant_id = "1"  # Default to plant 1
        
        self._last_restore_error_time = 0
        self._last_poll_error_time = 0
        
        # Mark as initialized
        self.initialized = True
        
    async def initialize(self):
        """Initialize the polling service and restore tasks"""
        # Ensure scheduler is started
        self.scheduler.start()
        logger.info("Started scheduler for periodic polling")
        
        # Restore polling tasks from database
        await self.restore_polling_tasks()
        
    def ensure_valid_node_id(self, node_id):
        """Ensure node ID has the correct format"""
        if not node_id:
            return None
        
        # No modification to node ID, use as is
        return node_id
        
    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    async def restore_polling_tasks(self):
        """Restore polling tasks from database for all plants"""
        try:
            logger.info("Restoring polling tasks from database for all plants...")
            
            # Get all plants from the registry
            from database import get_central_db
            from sqlalchemy import text
            
            plant_ids = []
            async for session in get_central_db():
                query = text("SELECT id FROM plants_registry WHERE is_active = true")
                result = await session.execute(query)
                plants = result.fetchall()
                plant_ids = [str(plant.id) for plant in plants]
                break
            
            if not plant_ids:
                logger.warning("No active plants found in registry")
                return
            
            logger.info(f"Found {len(plant_ids)} active plants: {plant_ids}")
            
            total_restored = 0
            
            # Restore tasks for each plant
            for plant_id in plant_ids:
                try:
                    # Get database session for the plant
                    async for session in get_plant_db(plant_id):
                        # Get all active polling tasks from database
                        tasks = await get_active_polling_tasks(session, plant_id)
                        
                        if not tasks:
                            logger.info(f"No active polling tasks found in database for plant {plant_id}")
                            break
                        
                        # Restore each polling task
                        restored_count = 0
                        for task in tasks:
                            try:
                                # Use the connection_string as the node_id instead of tag_name
                                # This allows the system to work with the new database schema
                                node_id = task['connection_string']
                                
                                # Validate OPC UA connection string format
                                if not validate_opcua_connection_string(node_id):
                                    logger.warning(f"Skipping restoration of polling task for invalid OPC UA connection string: {node_id}. Expected format: ns=<namespace>;<identifier_type>=<identifier>")
                                    continue
                                
                                # Skip if already polling
                                if plant_id in self.polling_tasks and node_id in self.polling_tasks[plant_id]:
                                    logger.info(f"Node {node_id} is already being polled in plant {plant_id}, skipping")
                                    continue
                                
                                # Add polling task with plant_id
                                logger.info(f"Restoring polling for connection_string {node_id} (tag: {task['tag_name']}) with interval {task['interval_seconds']}s in plant {plant_id}")
                                success = await self.add_polling_node(node_id, task['interval_seconds'], plant_id)
                                
                                if success:
                                    restored_count += 1
                                else:
                                    logger.warning(f"Failed to restore polling for connection_string {node_id} (tag: {task['tag_name']}) in plant {plant_id}")
                            except Exception as e:
                                logger.error(f"Error restoring polling task: {task}: {e}")
                        
                        logger.info(f"Restored {restored_count} polling tasks from database for plant {plant_id}")
                        total_restored += restored_count
                        break  # Only use the first session
                        
                except Exception as e:
                    logger.error(f"Error restoring polling tasks for plant {plant_id}: {e}")
                    continue
            
            logger.info(f"Total restored {total_restored} polling tasks across all plants")
                
        except Exception as e:
            now = time.time()
            if now - self._last_restore_error_time > 60:
                logger.error(f"Error restoring polling tasks: {e}")
                import traceback
                logger.error(traceback.format_exc())
                self._last_restore_error_time = now
            
    async def _fetch_and_save_node_data(self, node_id, plant_id=None):
        """Fetch node value and save it to database"""
        try:
            # Validate OPC UA connection string format
            if not validate_opcua_connection_string(node_id):
                logger.warning(f"Skipping polling for invalid OPC UA connection string: {node_id}. Expected format: ns=<namespace>;<identifier_type>=<identifier>")
                return None
            
            # Use provided plant_id or get from stored task info
            if plant_id is None:
                # Try to find the plant_id from any plant's tasks
                for p_id, plant_tasks in self.polling_tasks.items():
                    if node_id in plant_tasks:
                        plant_id = p_id
                        break
                else:
                    plant_id = self.default_plant_id
            else:
                plant_id = plant_id or self.default_plant_id
                
            # Get node data from OPC UA client
            node_data = await self.opc_client.get_value_of_specific_node(node_id)
            
            if node_data:
                # Update last poll time
                if plant_id in self.polling_tasks and node_id in self.polling_tasks[plant_id]:
                    self.polling_tasks[plant_id][node_id]["last_poll"] = time.time()
                    # Get the polling interval to use as frequency
                    interval_seconds = self.polling_tasks[plant_id][node_id]["interval"]
                    frequency = f"{interval_seconds}s"
                    
                    # Update polling task timestamp in database
                    async for session in get_plant_db(plant_id):
                        await update_polling_task_timestamp(session, node_id, interval_seconds, plant_id)
                        break  # Only use the first session
                else:
                    frequency = "60s"  # Default frequency
                
                # Log the polled data
                logger.info(f"Polled data for node {node_id} in plant {plant_id}: value={node_data['value']}, timestamp={node_data['timestamp']}")
                
                # Get the tag_id and save data to database
                async for session in get_plant_db(plant_id):
                    # Save data to database
                    success = await save_plant_node_data_to_db(session, node_id, node_data, plant_id, frequency)
                    
                    # Add required fields to node_data for Kafka
                    node_data["tag_id"] = None  # Will be filled by the database query
                    node_data["tag_name"] = node_id
                    
                    # Send to Kafka using the new helper method
                    await kafka_service.send_node_data("test", node_data)
                    
                    if success:
                        logger.info(f"Successfully saved data for node {node_id} to database in plant {plant_id}")
                    else:
                        logger.warning(f"Failed to save data for node {node_id} to database in plant {plant_id}")
                    
                    break  # Only use the first session
                
                return node_data
            else:
                logger.warning(f"Failed to get data for node {node_id}")
        except Exception as e:
            now = time.time()
            if now - self._last_poll_error_time > 60:
                logger.error(f"Error polling node {node_id} in plant {plant_id}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                self._last_poll_error_time = now
    
    async def polling_job_runner(node_id, plant_id=None):
        service = get_polling_service()
        # If plant_id is not provided, try to find it from the stored task info
        if plant_id is None:
            for p_id, plant_tasks in service.polling_tasks.items():
                if node_id in plant_tasks:
                    plant_id = p_id
                    break
            else:
                plant_id = service.default_plant_id
        await service._fetch_and_save_node_data(node_id, plant_id)

    async def add_polling_node(self, node_id, interval_seconds=60, plant_id=None):
        """Add a node to periodic polling"""
        try:
            # Validate OPC UA connection string format
            if not validate_opcua_connection_string(node_id):
                logger.error(f"Cannot add polling for invalid OPC UA connection string: {node_id}. Expected format: ns=<namespace>;<identifier_type>=<identifier>")
                return False
            
            # Use provided plant_id or fall back to default
            plant_id = plant_id or self.default_plant_id
            
            # Use node ID as is without modification
            node_id = self.ensure_valid_node_id(node_id)
            
            # Initialize plant-specific storage if not exists
            if plant_id not in self.polling_tasks:
                self.polling_tasks[plant_id] = {}
            
            # If node is already being polled in this plant, remove it first
            if node_id in self.polling_tasks[plant_id]:
                await self.remove_polling_node(node_id, plant_id)
            
            # Save polling task to database first
            async for session in get_plant_db(plant_id):
                task_id = await save_polling_task(session, node_id, interval_seconds, plant_id)
                if not task_id:
                    logger.error(f"Failed to save polling task for node {node_id} in plant {plant_id}")
                    return False
                break  # Only use the first session
            
            # Create plant-specific job ID
            job_id = f"poll_{plant_id}_{node_id}"
            
            # Add job to scheduler using the module-level function
            self.scheduler.add_job(
                job_id=job_id,
                func=PollingService.polling_job_runner,
                interval_seconds=interval_seconds,
                args=[node_id, plant_id]  # Pass both node_id and plant_id
            )
            
            # Store job information with plant_id
            self.polling_tasks[plant_id][node_id] = {
                "job_id": job_id,
                "interval": interval_seconds,
                "last_poll": time.time(),
                "plant_id": plant_id
            }
            
            logger.info(f"Added polling for node {node_id} with interval {interval_seconds}s in plant {plant_id}")
            
            # Immediately fetch data for the first time
            await self._fetch_and_save_node_data(node_id, plant_id)
            
            return True
        except Exception as e:
            logger.error(f"Error adding polling for node {node_id} in plant {plant_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    async def remove_polling_node(self, node_id, plant_id=None):
        """Remove a node from periodic polling"""
        try:
            # Use provided plant_id or get from stored task info
            if plant_id is None:
                # Try to find the plant_id from any plant's tasks
                for p_id, plant_tasks in self.polling_tasks.items():
                    if node_id in plant_tasks:
                        plant_id = p_id
                        break
                else:
                    plant_id = self.default_plant_id
            else:
                plant_id = plant_id or self.default_plant_id
                
            if plant_id in self.polling_tasks and node_id in self.polling_tasks[plant_id]:
                # Get job ID
                job_id = self.polling_tasks[plant_id][node_id]["job_id"]
                
                # Remove job from scheduler
                self.scheduler.remove_job(job_id)
                
                # Remove job information
                del self.polling_tasks[plant_id][node_id]
                
                # Clean up empty plant entry
                if not self.polling_tasks[plant_id]:
                    del self.polling_tasks[plant_id]
                
                # Deactivate polling task in database
                async for session in get_plant_db(plant_id):
                    await deactivate_polling_task(session, node_id, plant_id)
                    break  # Only use the first session
                
                logger.info(f"Removed polling for node {node_id} in plant {plant_id}")
                return True
            else:
                logger.warning(f"Polling task for node {node_id} not found in plant {plant_id}")
                return False
        except Exception as e:
            logger.error(f"Error removing polling for node {node_id} in plant {plant_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False 
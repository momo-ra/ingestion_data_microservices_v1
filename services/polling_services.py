import asyncio
import time
import threading
from utils.log import setup_logger
from services.scheduler_services import SchedulerService
from queries.polling_queries import save_polling_task, deactivate_polling_task, update_polling_task_timestamp, get_active_polling_tasks, get_or_create_tag_id
from queries.timeseries_queries import save_node_data_to_db
from services.kafka_services import kafka_service
from services.opc_ua_services import get_opc_ua_client
from tenacity import retry, stop_after_attempt, wait_fixed

logger = setup_logger(__name__)

# Singleton instance
_polling_service_instance = None
_polling_service_lock = threading.Lock()

def get_polling_service():
    """Get the singleton instance of PollingService"""
    return PollingService.get_instance()

class PollingService:
    @classmethod
    def get_instance(cls):
        """Get or create the singleton instance of PollingService"""
        global _polling_service_instance
        with _polling_service_lock:
            if _polling_service_instance is None:
                _polling_service_instance = cls()
        return _polling_service_instance
        
    def __init__(self):
        """Initialize the polling service"""
        # Initialize polling tasks
        self.polling_tasks = {}  # Dictionary to store polling task information
        
        # Get scheduler instance
        self.scheduler = SchedulerService.get_instance()
        
        # Get OPC UA client
        self.opc_client = get_opc_ua_client()
        
        self._last_restore_error_time = 0
        self._last_poll_error_time = 0
        
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
        """Restore polling tasks from database"""
        try:
            logger.info("Restoring polling tasks from database...")
            
            # Get all active polling tasks from database
            tasks = await get_active_polling_tasks()
            
            if not tasks:
                logger.info("No active polling tasks found in database")
                return
            
            # Restore each polling task
            restored_count = 0
            for task in tasks:
                try:
                    # Use the tag name directly as the node_id without modification
                    # This assumes the tag_name in the database is already in the correct format
                    node_id = task['tag_name']
                    
                    # Skip if already polling
                    if node_id in self.polling_tasks:
                        logger.info(f"Node {node_id} is already being polled, skipping")
                        continue
                    
                    # Add polling task
                    logger.info(f"Restoring polling for node {node_id} with interval {task['interval_seconds']}s")
                    success = await self.add_polling_node(node_id, task['interval_seconds'])
                    
                    if success:
                        restored_count += 1
                    else:
                        logger.warning(f"Failed to restore polling for node {node_id}")
                except Exception as e:
                    logger.error(f"Error restoring polling task: {task}: {e}")
            
            logger.info(f"Restored {restored_count} polling tasks from database")
        except Exception as e:
            now = time.time()
            if now - self._last_restore_error_time > 60:
                logger.error(f"Error restoring polling tasks: {e}")
                import traceback
                logger.error(traceback.format_exc())
                self._last_restore_error_time = now
            
    async def _fetch_and_save_node_data(self, node_id):
        """Fetch node value and save it to database"""
        try:
            # Get node data from OPC UA client
            node_data = await self.opc_client.get_value_of_specific_node(node_id)
            
            if node_data:
                # Update last poll time
                if node_id in self.polling_tasks:
                    self.polling_tasks[node_id]["last_poll"] = time.time()
                    # Get the polling interval to use as frequency
                    interval_seconds = self.polling_tasks[node_id]["interval"]
                    frequency = f"{interval_seconds}s"
                    
                    # Update polling task timestamp in database - use node_id directly
                    await update_polling_task_timestamp(node_id, interval_seconds)
                else:
                    frequency = "60s"  # Default frequency
                
                # Log the polled data
                logger.info(f"Polled data for node {node_id}: value={node_data['value']}, timestamp={node_data['timestamp']}")
                
                # Get the tag_id from the database to ensure consistency
                tag_id = await get_or_create_tag_id(node_id)
                
                # Save data to database - pass node_id directly
                success = await save_node_data_to_db(node_id, node_data, frequency)
                
                # Add required fields to node_data for Kafka
                node_data["tag_id"] = tag_id
                node_data["tag_name"] = node_id
                
                # Send to Kafka using the new helper method
                await kafka_service.send_node_data("test", node_data)
                
                if success:
                    logger.info(f"Successfully saved data for node {node_id} to database")
                else:
                    logger.warning(f"Failed to save data for node {node_id} to database")
                
                return node_data
            else:
                logger.warning(f"Failed to get data for node {node_id}")
        except Exception as e:
            now = time.time()
            if now - self._last_poll_error_time > 60:
                logger.error(f"Error polling node {node_id}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                self._last_poll_error_time = now
    
    async def polling_job_runner(node_id):
        service = get_polling_service()
        await service._fetch_and_save_node_data(node_id)

    async def add_polling_node(self, node_id, interval_seconds=60):
        """Add a node to periodic polling"""
        try:
            # Use node ID as is without modification
            node_id = self.ensure_valid_node_id(node_id)
            
            # If node is already being polled, remove it first
            if node_id in self.polling_tasks:
                await self.remove_polling_node(node_id)
            
            # Save polling task to database first
            task_id = await save_polling_task(node_id, interval_seconds)
            if not task_id:
                logger.error(f"Failed to save polling task for node {node_id}")
                return False
            
            # Create job ID
            job_id = f"poll_{node_id}"
            
            # Add job to scheduler using the module-level function
            self.scheduler.add_job(
                job_id=job_id,
                func=PollingService.polling_job_runner,
                interval_seconds=interval_seconds,
                args=[node_id]
            )
            
            # Store job information
            self.polling_tasks[node_id] = {
                "job_id": job_id,
                "interval": interval_seconds,
                "last_poll": time.time()
            }
            
            logger.info(f"Added polling for node {node_id} with interval {interval_seconds}s")
            
            # Immediately fetch data for the first time
            await self._fetch_and_save_node_data(node_id)
            
            return True
        except Exception as e:
            logger.error(f"Error adding polling for node {node_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    async def remove_polling_node(self, node_id):
    
        """Remove a node from periodic polling"""
        try:
            if node_id in self.polling_tasks:
                # Get job ID
                job_id = self.polling_tasks[node_id]["job_id"]
                
                # Remove job from scheduler
                self.scheduler.remove_job(job_id)
                
                # Remove job information
                del self.polling_tasks[node_id]
                
                # Deactivate polling task in database
                await deactivate_polling_task(node_id)
                
                logger.info(f"Removed polling for node {node_id}")
                return True
            else:
                logger.warning(f"Polling task for node {node_id} not found")
                return False
        except Exception as e:
            logger.error(f"Error removing polling for node {node_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False 
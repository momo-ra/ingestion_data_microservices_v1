"""
Subscription Services

This module provides services for managing OPC UA subscriptions.
Updated for multi-database architecture with plant-specific databases.
"""

from utils.log import setup_logger
from utils.error_handling import handle_async_errors, SubscriptionError
from utils.metrics import subscription_count
from services.opc_ua_services import get_opc_ua_client
from services.kafka_services import kafka_service
from queries.timeseries_queries import save_node_data_to_db
from queries.subscription_queries import save_subscription_task, deactivate_subscription_task, get_active_subscription_tasks, get_or_create_tag_id
from datetime import datetime
from schemas.schema import TagSchema, TimeSeriesSchema
from database import get_plant_db
from utils.singleton import Singleton

logger = setup_logger(__name__)

# SubHandler for handling data change notifications
class SubHandler(object):    
    def __init__(self, subscription_service):
        self.subscription_service = subscription_service
        
    async def datachange_notification(self, node, val, data):
        """Handle data change notifications from OPC UA server"""
        try:
            logger.info(f"Received data change: node={node}, val={val}")
            
            # Process the data change
            await self.subscription_service.process_data_change(node, val, data)
        except Exception as e:
            logger.error(f"Error in datachange_notification: {e}")
            import traceback
            logger.error(traceback.format_exc())

# Singleton instance
_subscription_service_instance = None

def get_subscription_service():
    """Get the singleton instance of SubscriptionService"""
    return SubscriptionService.get_instance()

class SubscriptionService(Singleton):
    """Subscription service with database integration"""
    
    @classmethod
    def get_instance(cls):
        """Get the singleton instance"""
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance
        
    def __init__(self):
        """Initialize the subscription service"""
        # Avoid re-initialization if already initialized
        if hasattr(self, 'initialized') and self.initialized:
            return
            
        self.opc_client = get_opc_ua_client()
        self.subscription = None
        self.subscription_handles = {}  # Dictionary to store subscription handles
        self.handler = SubHandler(self)
        
        # Default plant and workspace for now - should be configurable
        self.default_plant_id = "1"  # Default to plant 1
        self.default_workspace_id = 1  # Default to workspace 1
        
        # Mark as initialized
        self.initialized = True
        
    @handle_async_errors(error_class=SubscriptionError, default_message="Error initializing subscription service")
    async def initialize(self):
        """Initialize the subscription service and restore subscriptions"""
        try:
            # Ensure OPC UA client is connected
            if not self.opc_client.connected:
                success = await self.opc_client.connect()
                if not success:
                    logger.error("Failed to connect to OPC UA server for subscription initialization")
                    return False
            
            # Create subscription if not already created
            if self.subscription is None:
                self.subscription = await self.opc_client.client.create_subscription(500, self.handler)
                logger.info("Created OPC UA subscription")
            
            # Restore subscriptions from database
            await self.restore_subscriptions()
            
            # Update metrics
            subscription_count.set(len(self.subscription_handles))
            
            logger.info("Subscription service initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Error initializing subscription service: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise SubscriptionError(
                message=f"Failed to initialize subscription service: {e}",
                error_code="INITIALIZATION_ERROR"
            )
    
    @handle_async_errors(error_class=SubscriptionError, default_message="Error cleaning up subscriptions")
    async def cleanup(self):
        """Clean up subscriptions"""
        try:
            if self.subscription is not None:
                try:
                    # Get the subscription ID properly
                    if hasattr(self.subscription, 'subscription_id'):
                        subscription_id = self.subscription.subscription_id
                        await self.opc_client.client.delete_subscriptions([subscription_id])
                    else:
                        # Try a safe alternative approach
                        logger.warning("Subscription object doesn't have subscription_id attribute, trying alternative cleanup")
                        if hasattr(self.subscription, 'delete'):
                            await self.subscription.delete()
                except Exception as e:
                    logger.warning(f"Error deleting subscription: {e}, continuing cleanup")
                
                # Always reset our internal state, even if deletion failed
                self.subscription = None
                self.subscription_handles = {}
                
                # Update metrics
                subscription_count.set(0)
                
                logger.info("Cleaned up OPC UA subscriptions")
        except Exception as e:
            logger.error(f"Error cleaning up subscriptions: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise SubscriptionError(
                message=f"Failed to cleanup subscriptions: {e}",
                error_code="CLEANUP_ERROR"
            )
    
    @handle_async_errors(error_class=SubscriptionError, default_message="Error creating subscription")
    async def create_subscription(self, node_id):
        """Create a subscription for a specific node

        Args:
            node_id: The OPC UA node ID

        Returns:
            Handle to the subscription or None if failed
        """
        try:
            # Initialize if not already initialized
            if self.subscription is None:
                success = await self.initialize()
                if not success:
                    return None
            
            # Check if already subscribed
            if node_id in self.subscription_handles:
                logger.info(f"Already subscribed to node {node_id}")
                return self.subscription_handles[node_id]
            
            # Get the node
            node = self.opc_client.client.get_node(node_id)
            
            # Subscribe to data changes
            handle = await self.subscription.subscribe_data_change(
                nodes=node
            )
            
            # Store the subscription handle
            self.subscription_handles[node_id] = handle
            
            # Save subscription to database
            async for session in get_plant_db(self.default_plant_id):
                await save_subscription_task(session, self.default_workspace_id, node_id, self.default_plant_id)
                break  # Only use the first session
            
            # Update metrics
            subscription_count.set(len(self.subscription_handles))
            
            logger.info(f"Created subscription for node {node_id}")
            
            return handle
        except Exception as e:
            logger.error(f"Error creating subscription for node {node_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise SubscriptionError(
                message=f"Failed to create subscription for node {node_id}: {e}",
                error_code="SUBSCRIPTION_CREATE_ERROR",
                details={"node_id": node_id}
            )
    
    @handle_async_errors(error_class=SubscriptionError, default_message="Error removing subscription")
    async def remove_subscription(self, node_id):
        """Remove a subscription for a specific node

        Args:
            node_id: The OPC UA node ID

        Returns:
            bool: True if successfully removed, False otherwise
        """
        try:
            # Check if we have a valid subscription
            if self.subscription is None:
                logger.warning("OPC UA subscription is not initialized, cannot remove subscription")
                return False
                
            if node_id in self.subscription_handles:
                # Get the server handle for this node_id
                server_handle = self.subscription_handles[node_id]
                logger.info(f"Found server handle {server_handle} for node {node_id}")
                
                # Unsubscribe using the server handle
                await self.subscription.unsubscribe(server_handle)
                
                # Remove from our tracking dictionary
                del self.subscription_handles[node_id]
                
                # Deactivate subscription in database
                async for session in get_plant_db(self.default_plant_id):
                    await deactivate_subscription_task(session, self.default_workspace_id, node_id, self.default_plant_id)
                    break  # Only use the first session
                
                # Update metrics
                subscription_count.set(len(self.subscription_handles))
                
                logger.info(f"Successfully removed subscription for node {node_id}")
                return True
            else:
                logger.warning(f"Subscription for node {node_id} not found in handles: {self.subscription_handles}")
                return False
        except Exception as e:
            logger.error(f"Error removing subscription for node {node_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise SubscriptionError(
                message=f"Failed to remove subscription for node {node_id}: {e}",
                error_code="SUBSCRIPTION_REMOVE_ERROR",
                details={"node_id": node_id}
            )
    
    @handle_async_errors(error_class=SubscriptionError, default_message="Error restoring subscriptions")
    async def restore_subscriptions(self):
        """Restore subscriptions from the database"""
        try:
            logger.info("Restoring subscriptions from database...")
            
            # Get database session for the default plant
            async for session in get_plant_db(self.default_plant_id):
                # Get all active subscription tasks from database
                tasks = await get_active_subscription_tasks(session, self.default_workspace_id, self.default_plant_id)
                
                if not tasks:
                    logger.info("No active subscriptions found in database")
                    return
                
                # Restore each subscription
                restored_count = 0
                for task in tasks:
                    try:
                        node_id = task['tag_name']
                        
                        # Skip if already subscribed
                        if node_id in self.subscription_handles:
                            logger.info(f"Already subscribed to node {node_id}, skipping")
                            continue
                        
                        # Create subscription
                        logger.info(f"Restoring subscription for node {node_id}")
                        handle = await self.create_subscription(node_id)
                        
                        if handle:
                            restored_count += 1
                        else:
                            logger.warning(f"Failed to restore subscription for node {node_id}")
                    except Exception as e:
                        logger.error(f"Error restoring subscription for node {task['tag_name']}: {e}")
                
                logger.info(f"Restored {restored_count} subscriptions from database")
                break  # Only use the first session
                
        except Exception as e:
            logger.error(f"Error restoring subscriptions: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise SubscriptionError(
                message=f"Failed to restore subscriptions: {e}",
                error_code="SUBSCRIPTION_RESTORE_ERROR"
            )
    
    @handle_async_errors(error_class=SubscriptionError, default_message="Error processing data change")
    async def process_data_change(self, node, val, data):
        """Process data change from subscription

        Args:
            node: The OPC UA node
            val: The new value
            data: The data change event data

        Returns:
            bool: True if successfully processed, False otherwise
        """
        try:
            # Get node ID
            node_id = node.nodeid.to_string()
            
            # Create node data
            # Check if data has SourceTimestamp attribute, otherwise use current timestamp
            try:
                timestamp = data.SourceTimestamp.isoformat() if hasattr(data, 'SourceTimestamp') and data.SourceTimestamp else None
            except AttributeError:
                # Use current time if SourceTimestamp not available
                timestamp = datetime.now().isoformat()
                logger.info(f"SourceTimestamp not available for node {node_id}, using current time")
            
            # Get status code if available, otherwise set to None
            status = getattr(data, 'StatusCode', None)
            
            # Get the tag_id and save data to database
            async for session in get_plant_db(self.default_plant_id):
                # Get the tag_id from the database to ensure consistency
                tag_id = await get_or_create_tag_id(session, node_id, self.default_plant_id)
                
                node_data = {
                    "node_id": node_id,
                    "tag_id": tag_id,
                    "tag_name": node_id,
                    "value": val,
                    "timestamp": timestamp,
                    "status": str(status) if status else "Good"
                }
                
                # Log the data
                logger.info(f"Subscription data for node {node_id}: value={val}, timestamp={timestamp}")
                
                # Save to database with default frequency for subscriptions
                frequency = "sub"  # Special marker for subscription data
                success = await save_node_data_to_db(session, node_id, node_data, self.default_plant_id, frequency)
                
                # Send to Kafka using the new helper method
                await kafka_service.send_node_data("test", node_data)
                
                if success:
                    logger.info(f"Successfully saved subscription data for node {node_id} to database")
                else:
                    logger.warning(f"Failed to save subscription data for node {node_id} to database")
                
                break  # Only use the first session
            
            return True
        except Exception as e:
            logger.error(f"Error processing data change: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise SubscriptionError(
                message=f"Failed to process data change: {e}",
                error_code="DATA_CHANGE_PROCESS_ERROR"
            ) 
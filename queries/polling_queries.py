from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession
from models.plant_models import Tag, TimeSeries, PollingTasks
from datetime import datetime, timedelta
from utils.log import setup_logger
from queries.tag_queries import get_or_create_tag_id, get_tag_by_name, validate_opcua_connection_string

logger = setup_logger(__name__)

async def save_polling_task(session: AsyncSession, node_id: str, interval_seconds: int, plant_id: str):
    """Save a polling task to the plant database
    
    Args:
        session: Database session for the specific plant
        node_id (str): The OPC UA connection string (e.g., "ns=3;i=1002")
        interval_seconds (int): The polling interval in seconds
        plant_id (str): The plant ID for logging
        
    Returns:
        int: The polling task ID
    """
    try: 
        # Validate OPC UA connection string format
        if not validate_opcua_connection_string(node_id):
            logger.error(f"Invalid OPC UA connection string format: {node_id}. Expected format: ns=<namespace>;<identifier_type>=<identifier>")
            return None
        
        # Find tag by connection_string instead of name
        result = await session.execute(
            select(Tag).where(Tag.connection_string == node_id)
        )
        tag = result.scalars().first()
        
        if not tag:
            logger.error(f"No tag found with connection_string {node_id} in plant {plant_id}")
            return None
        
        tag_id = tag.id
        
        # Calculate next poll time
        now = datetime.now()
        next_poll = now + timedelta(seconds=interval_seconds)
        
        # Check if task already exists
        result = await session.execute(
            select(PollingTasks).where(
                (PollingTasks.tag_id == tag_id) & 
                (PollingTasks.time_interval == interval_seconds)
            )
        )
        existing_task = result.scalars().first()
        
        if existing_task:
            # Update existing task
            existing_task.is_active = True
            existing_task.last_polled = now
            existing_task.next_polled = next_poll
            await session.commit()
            logger.info(f"Updated polling task for connection_string {node_id} (tag: {tag.name}) in plant {plant_id} with interval {interval_seconds}s")
            return existing_task.id
        else:
            # Create new task
            new_task = PollingTasks(
                tag_id=tag_id,
                time_interval=interval_seconds,
                is_active=True,
                last_polled=now,
                next_polled=next_poll
            )
            session.add(new_task)
            await session.commit()
            await session.refresh(new_task)
            logger.info(f"Created polling task for connection_string {node_id} (tag: {tag.name}) in plant {plant_id} with interval {interval_seconds}s")
            return new_task.id
    except Exception as e:
        logger.error(f"Error saving polling task for connection_string {node_id} in plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await session.rollback()
        return None

async def deactivate_polling_task(session: AsyncSession, node_id: str, plant_id: str, interval_seconds: int = None):
    """Deactivate a polling task in the plant database
    
    Args:
        session: Database session for the specific plant
        node_id (str): The OPC UA connection string (e.g., "ns=3;i=1002")
        plant_id (str): The plant ID for logging
        interval_seconds (int, optional): The polling interval in seconds. If None, deactivate all tasks for this node.
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Validate OPC UA connection string format
        if not validate_opcua_connection_string(node_id):
            logger.error(f"Invalid OPC UA connection string format: {node_id}. Expected format: ns=<namespace>;<identifier_type>=<identifier>")
            return False
        
        # Log what we're trying to deactivate
        if interval_seconds is None:
            logger.info(f"Attempting to deactivate all polling tasks for connection_string {node_id} in plant {plant_id}")
        else:
            logger.info(f"Attempting to deactivate polling task for connection_string {node_id} in plant {plant_id} with interval {interval_seconds}s")
        
        # Find tag by connection_string instead of name
        result = await session.execute(
            select(Tag).where(Tag.connection_string == node_id)
        )
        tag = result.scalars().first()
        
        if not tag:
            logger.warning(f"No tag found with connection_string {node_id} in plant {plant_id}")
            return False
        
        tag_id = tag.id
        logger.info(f"Found tag {tag.name} (ID: {tag_id}) with connection_string {node_id}")
        
        # Deactivate the polling task(s)
        if interval_seconds is None:
            # Deactivate all tasks for this tag
            query = (
                update(PollingTasks)
                .where(PollingTasks.tag_id == tag_id)
                .values(is_active=False)
            )
        else:
            # Deactivate specific task
            query = (
                update(PollingTasks)
                .where(
                    (PollingTasks.tag_id == tag_id) & 
                    (PollingTasks.time_interval == interval_seconds)
                )
                .values(is_active=False)
            )
        
        result = await session.execute(query)
        await session.commit()
        
        if result.rowcount > 0:
            logger.info(f"Deactivated {result.rowcount} polling task(s) for tag {tag.name} (connection_string: {node_id}) in plant {plant_id}")
            return True
        else:
            logger.warning(f"No active polling tasks found for tag {tag.name} (connection_string: {node_id}) in plant {plant_id}")
            return False
        
    except Exception as e:
        logger.error(f"Error deactivating polling task for connection_string {node_id} in plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await session.rollback()
        return False

async def update_polling_task_timestamp(session: AsyncSession, node_id: str, interval_seconds: int, plant_id: str):
    """Update the last_polled and next_polled timestamps for a polling task
    
    Args:
        session: Database session for the specific plant
        node_id (str): The OPC UA connection string (e.g., "ns=3;i=1002")
        interval_seconds (int): The polling interval in seconds
        plant_id (str): The plant ID for logging
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Validate OPC UA connection string format
        if not validate_opcua_connection_string(node_id):
            logger.error(f"Invalid OPC UA connection string format: {node_id}. Expected format: ns=<namespace>;<identifier_type>=<identifier>")
            return False
        
        # Find tag by connection_string instead of name
        result = await session.execute(
            select(Tag).where(Tag.connection_string == node_id)
        )
        tag = result.scalars().first()
        
        if not tag:
            logger.warning(f"No tag found with connection_string {node_id} in plant {plant_id}")
            return False
        
        tag_id = tag.id
        
        # Calculate timestamps
        now = datetime.now()
        next_poll = now + timedelta(seconds=interval_seconds)
        
        query = (
            update(PollingTasks)
            .where(
                (PollingTasks.tag_id == tag_id) & 
                (PollingTasks.time_interval == interval_seconds) &
                (PollingTasks.is_active == True)
            )
            .values(last_polled=now, next_polled=next_poll)
        )
        
        result = await session.execute(query)
        await session.commit()
        
        if result.rowcount > 0:
            logger.debug(f"Updated timestamps for polling task: connection_string={node_id}, tag={tag.name}, plant={plant_id}, interval={interval_seconds}s")
            return True
        else:
            logger.warning(f"No active polling task found for tag {tag.name} (connection_string: {node_id}) in plant {plant_id} with interval {interval_seconds}s")
            return False
    except Exception as e:
        logger.error(f"Error updating timestamps for polling task: connection_string={node_id}, plant={plant_id}, interval={interval_seconds}s: {e}")
        await session.rollback()
        return False

async def get_active_polling_tasks(session: AsyncSession, plant_id: str = None):
    """Get active polling tasks from the plant database
    
    Args:
        session: Database session for the specific plant
        plant_id: Plant ID for filtering and logging
        
    Returns:
        list: List of active polling tasks with tag information
    """
    try:
        # Build query - join polling_tasks with Tag to get tag name and connection_string
        query = select(
            PollingTasks, 
            Tag.name.label("tag_name"), 
            Tag.connection_string.label("connection_string")
        ).join(
            Tag, PollingTasks.tag_id == Tag.id
        ).where(
            (PollingTasks.is_active == True) & 
            (Tag.plant_id == int(plant_id) if plant_id and plant_id.isdigit() else True)
        )
        
        result = await session.execute(query)
        tasks = result.all()
        
        # Convert to list of dictionaries
        task_list = []
        for task, tag_name, connection_string in tasks:
            task_dict = {
                "id": task.id,
                "tag_id": task.tag_id,
                "tag_name": tag_name,
                "connection_string": connection_string,
                "interval_seconds": task.time_interval,
                "last_polled": task.last_polled,
                "next_polled": task.next_polled
            }
            task_list.append(task_dict)
        
        if plant_id:
            logger.debug(f"Retrieved {len(task_list)} active polling tasks from plant {plant_id}")
        
        return task_list
    except Exception as e:
        logger.error(f"Error getting active polling tasks from plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []
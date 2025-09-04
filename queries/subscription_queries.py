"""
Subscription Queries

This module provides database queries related to subscription tasks.
Updated for multi-database architecture with plant-specific databases.
"""

from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession
from models.plant_models import Tag, SubscriptionTasks
from datetime import datetime
from utils.log import setup_logger

logger = setup_logger(__name__)

async def save_subscription_task(session: AsyncSession, workspace_id: int, node_id: str, plant_id: str):
    """Save a subscription task to the plant database
    
    Args:
        session: Database session for the specific plant
        workspace_id: The workspace ID
        node_id (str): The node ID
        plant_id (str): The plant ID for logging
        
    Returns:
        int: The subscription task ID or None if failed
    """
    try: 
        # Get tag_id
        tag_id = await get_or_create_tag_id(session, node_id, plant_id)
        
        if not tag_id:
            logger.error(f"Could not get or create tag for node {node_id} in plant {plant_id}")
            return None
        
        # Check if task already exists
        result = await session.execute(
            select(SubscriptionTasks).where(
                (SubscriptionTasks.workspace_id == workspace_id) &
                (SubscriptionTasks.tag_id == tag_id)
            )
        )
        existing_task = result.scalars().first()
        
        if existing_task:
            # Update existing task
            existing_task.is_active = True
            existing_task.last_updated = datetime.now()
            await session.commit()
            logger.info(f"Updated subscription task for node {node_id} in workspace {workspace_id}, plant {plant_id}")
            return existing_task.id
        else:
            # Create new task
            new_task = SubscriptionTasks(
                workspace_id=workspace_id,
                tag_id=tag_id,
                is_active=True,
                last_updated=datetime.now()
            )
            session.add(new_task)
            await session.commit()
            await session.refresh(new_task)
            logger.info(f"Created subscription task for node {node_id} in workspace {workspace_id}, plant {plant_id}")
            return new_task.id
    except Exception as e:
        logger.error(f"Error saving subscription task for node {node_id} in workspace {workspace_id}, plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await session.rollback()
        return None

async def deactivate_subscription_task(session: AsyncSession, workspace_id: int, node_id: str, plant_id: str):
    """Deactivate a subscription task in the plant database
    
    Args:
        session: Database session for the specific plant
        workspace_id: The workspace ID
        node_id (str): The node ID
        plant_id (str): The plant ID for logging
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Use node_id directly as tag_name without parsing
        tag_name = node_id
        
        # Also try to get results using any extracted tag name (for backward compatibility)
        alternate_tag_names = []
        
        # Extract tag name from node_id if it has a specific format
        parts = node_id.split(';')
        if len(parts) > 1:
            try:
                extracted_tag = parts[1].split('=')[1]
                if extracted_tag != tag_name:
                    alternate_tag_names.append(extracted_tag)
            except (IndexError, ValueError):
                # If parsing fails, just continue with the original tag_name
                pass
                
        # Log what we're trying to deactivate
        logger.info(f"Attempting to deactivate subscription for node {node_id} in workspace {workspace_id}, plant {plant_id} (tag names: {[tag_name] + alternate_tag_names})")
            
        # Get tag_ids for all possible tag names
        tag_ids = []
        primary_tag_id = await get_or_create_tag_id(session, tag_name, plant_id)
        if primary_tag_id:
            tag_ids.append(primary_tag_id)
            
        for alt_tag in alternate_tag_names:
            alt_tag_id = await get_or_create_tag_id(session, alt_tag, plant_id)
            if alt_tag_id and alt_tag_id not in tag_ids:
                tag_ids.append(alt_tag_id)
                
        if not tag_ids:
            logger.warning(f"No tags found for node {node_id} in plant {plant_id}")
            return False
            
        # Use all possible tag IDs to ensure we find and deactivate the task
        success = False
        for tag_id in tag_ids:
            # Deactivate subscription for this tag in this workspace
            query = (
                update(SubscriptionTasks)
                .where(
                    (SubscriptionTasks.workspace_id == workspace_id) &
                    (SubscriptionTasks.tag_id == tag_id)
                )
                .values(is_active=False, last_updated=datetime.now())
            )
            
            result = await session.execute(query)
            await session.commit()
            
            if result.rowcount > 0:
                logger.info(f"Deactivated {result.rowcount} subscription task(s) for tag_id {tag_id} in workspace {workspace_id}, plant {plant_id}")
                success = True
        
        return success
    except Exception as e:
        logger.error(f"Error deactivating subscription task for node {node_id} in workspace {workspace_id}, plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await session.rollback()
        return False

async def get_active_subscription_tasks(session: AsyncSession, workspace_id: int = None, plant_id: str = None):
    """Get active subscription tasks from the plant database
    
    Args:
        session: Database session for the specific plant
        workspace_id: Optional workspace ID to filter by
        plant_id: Plant ID for logging
        
    Returns:
        list: List of active subscription tasks with tag information
    """
    try:
        # Build query - join subscription_tasks with Tag to get tag name
        query = select(SubscriptionTasks, Tag.name.label("tag_name")).join(
            Tag, SubscriptionTasks.tag_id == Tag.id
        ).where(SubscriptionTasks.is_active == True)
        
        # Add workspace filter if provided
        if workspace_id is not None:
            query = query.where(SubscriptionTasks.workspace_id == workspace_id)
        
        result = await session.execute(query)
        tasks = result.all()
        
        # Convert to list of dictionaries
        task_list = []
        for task, tag_name in tasks:
            task_dict = {
                "id": task.id,
                "workspace_id": task.workspace_id,
                "tag_id": task.tag_id,
                "tag_name": tag_name,
                "created_at": task.created_at,
                "last_updated": task.last_updated
            }
            task_list.append(task_dict)
        
        if plant_id:
            logger.debug(f"Retrieved {len(task_list)} active subscription tasks from plant {plant_id}" + 
                        (f" workspace {workspace_id}" if workspace_id else ""))
        
        return task_list
    except Exception as e:
        logger.error(f"Error getting active subscription tasks from plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

async def get_or_create_tag_id(session: AsyncSession, node_id: str, plant_id: str):
    """Get tag ID by name or create a new tag if it doesn't exist
    
    Args:
        session: Database session for the specific plant
        node_id (str): The OPC-UA node ID used as tag name
        plant_id (str): The plant ID
        
    Returns:
        int: The tag ID or None if failed
    """
    try:
        # First check if the tag already exists in the plant database
        result = await session.execute(select(Tag).where(Tag.name == node_id))
        tag = result.scalars().first()
        
        if tag:
            return tag.id
        
        # Check if the node exists in the OPC UA server
        from services.opc_ua_services import get_opc_ua_client
        opc_client = get_opc_ua_client()
        node_exists = False
        
        try:
            if opc_client.connected:
                # Try to get the node from the server
                node = opc_client.client.get_node(node_id)
                # Try to read a property to verify the node exists
                await node.read_browse_name()
                node_exists = True
                logger.info(f"Verified node {node_id} exists in OPC UA server for plant {plant_id}")
            else:
                logger.warning(f"OPC UA client not connected, cannot verify node {node_id} for plant {plant_id}")
                # Don't proceed if we can't verify - return None to prevent tag creation
                node_exists = False
        except Exception as e:
            logger.warning(f"Node {node_id} not found in OPC UA server or error accessing it for plant {plant_id}: {e}")
            # Don't create tag if node doesn't exist
            node_exists = False
            
        if node_exists:
            # Create new tag if it doesn't exist in the database
            # Convert plant_id to int for the plant_id field
            plant_id_int = int(plant_id) if plant_id.isdigit() else 1  # Default to 1 if not numeric
            
            new_tag = Tag(
                name=node_id,
                description=f"Auto-created tag for {node_id}",
                unit_of_measure="unknown",
                plant_id=plant_id_int,
                is_active=True
            )
        
            session.add(new_tag)
            await session.commit()
            await session.refresh(new_tag)
        
            logger.info(f"Created new tag with ID {new_tag.id} for node {node_id} in plant {plant_id}")
            return new_tag.id
        else:
            logger.warning(f"Node {node_id} does not exist in OPC UA server, cannot create tag for plant {plant_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting or creating tag for node {node_id} in plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await session.rollback()
        return None 
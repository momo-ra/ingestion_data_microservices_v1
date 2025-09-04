from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from models.plant_models import Tag
from utils.log import setup_logger
from services.datasource_connection_manager import get_datasource_connection_manager
import re

logger = setup_logger(__name__)

def validate_opcua_connection_string(connection_string: str) -> bool:
    """Validate OPC UA connection string format
    
    Args:
        connection_string (str): The connection string to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    # OPC UA node ID format: ns=<namespace>;<identifier_type>=<identifier>
    # Examples: ns=3;i=1001, ns=2;s=MyVariable, ns=1;g=12345678-1234-1234-1234-123456789abc
    # Must end after the identifier, no extra parts allowed
    pattern = r'^ns=\d+;[isgb]=[^;]+$'
    return bool(re.match(pattern, connection_string))

async def get_or_create_tag_id(session: AsyncSession, name: str, plant_id: str, data_source_id: int, connection_string: str):
    """Get tag ID by name or create a new tag if it doesn't exist
    
    Args:
        session: Database session for the specific plant
        name (str): The tag name in the database
        plant_id (str): The plant ID
        data_source_id (int): The datasource ID
        connection_string (str): The connection string (node ID) to search in datasource
        
    Returns:
        int: The tag ID or None if failed
    """
    try:
        # First check if the tag already exists in the plant database
        result = await session.execute(
            select(Tag).where(
                Tag.name == name,
                Tag.data_source_id == data_source_id
            )
        )
        tag = result.scalars().first()
        
        if tag:
            return tag.id
        
        # Check if the node exists in the datasource using connection_string
        connection_manager = get_datasource_connection_manager()
        node_exists = False
        
        # Validate OPC UA connection string format
        if not validate_opcua_connection_string(connection_string):
            logger.warning(f"Invalid OPC UA connection string format: {connection_string}. Expected format: ns=<namespace>;<identifier_type>=<identifier>")
            return None
        
        try:
            # Test connection and verify node exists
            test_result = await connection_manager.test_connection(session, data_source_id, int(plant_id))
            if test_result["success"]:
                # Try to read the node to verify it exists using connection_string
                read_result = await connection_manager.read_node(session, data_source_id, int(plant_id), connection_string)
                if read_result["success"]:
                    node_exists = True
                    logger.info(f"Verified node {connection_string} exists in datasource {data_source_id} for plant {plant_id}")
                else:
                    logger.warning(f"Node {connection_string} not found in datasource {data_source_id} for plant {plant_id}: {read_result.get('error', 'Unknown error')}")
                    node_exists = False
            else:
                logger.warning(f"Datasource {data_source_id} not connected, cannot verify node {connection_string} for plant {plant_id}")
                node_exists = False
        except Exception as e:
            logger.warning(f"Error verifying node {connection_string} in datasource {data_source_id} for plant {plant_id}: {e}")
            node_exists = False
            
        if node_exists:
            # Create new tag if it doesn't exist in the database
            plant_id_int = int(plant_id) if plant_id.isdigit() else 1
            
            new_tag = Tag(
                name=name,
                connection_string=connection_string,
                description=f"Auto-created tag for {name}",
                unit_of_measure="unknown",
                plant_id=plant_id_int,
                data_source_id=data_source_id,
                is_active=True
            )
        
            session.add(new_tag)
            await session.commit()
            await session.refresh(new_tag)
        
            logger.info(f"Created new tag with ID {new_tag.id} for name {name} with connection_string {connection_string} in datasource {data_source_id} for plant {plant_id}")
            return new_tag.id
        else:
            logger.warning(f"Node {connection_string} does not exist in datasource {data_source_id}, cannot create tag for plant {plant_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting or creating tag for name {name} with connection_string {connection_string} in datasource {data_source_id} for plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await session.rollback()
        return None 

async def get_tag_by_id(session: AsyncSession, tag_id: int, plant_id: str):
    """Get tag by ID from the plant database
    
    Args:
        session: Database session for the specific plant
        tag_id (int): The tag ID
        plant_id (str): The plant ID for logging
        
    Returns:
        Tag: The tag object or None if not found
    """
    try:
        result = await session.execute(
            select(
                Tag.id,
                Tag.name,
                Tag.connection_string,
                Tag.description,
                Tag.unit_of_measure,
                Tag.plant_id,
                Tag.data_source_id,
                Tag.is_active,
                Tag.created_at,
                Tag.updated_at
            ).where(Tag.id == tag_id)
        )
        tag_row = result.first()
        
        if tag_row:
            tag_obj = Tag(
                id=tag_row.id,
                name=tag_row.name,
                connection_string=tag_row.connection_string,
                description=tag_row.description,
                unit_of_measure=tag_row.unit_of_measure,
                plant_id=tag_row.plant_id,
                data_source_id=tag_row.data_source_id,
                is_active=tag_row.is_active,
                created_at=tag_row.created_at,
                updated_at=tag_row.updated_at
            )
            logger.info(f"Retrieved tag {tag_id} for plant {plant_id}")
            return tag_obj
        else:
            logger.warning(f"Tag {tag_id} not found in plant {plant_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting tag {tag_id} in plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

async def get_tag_by_name(session: AsyncSession, tag_name: str, plant_id: str):
    """Get tag by name from the plant database
    
    Args:
        session: Database session for the specific plant
        tag_name (str): The tag name
        plant_id (str): The plant ID for logging
        
    Returns:
        Tag: The tag object or None if not found
    """
    try:
        result = await session.execute(
            select(
                Tag.id,
                Tag.name,
                Tag.connection_string,
                Tag.description,
                Tag.unit_of_measure,
                Tag.plant_id,
                Tag.data_source_id,
                Tag.is_active,
                Tag.created_at,
                Tag.updated_at
            ).where(Tag.name == tag_name)
        )
        tag_row = result.first()
        
        if tag_row:
            tag_obj = Tag(
                id=tag_row.id,
                name=tag_row.name,
                connection_string=tag_row.connection_string,
                description=tag_row.description,
                unit_of_measure=tag_row.unit_of_measure,
                plant_id=tag_row.plant_id,
                data_source_id=tag_row.data_source_id,
                is_active=tag_row.is_active,
                created_at=tag_row.created_at,
                updated_at=tag_row.updated_at
            )
            logger.info(f"Retrieved tag '{tag_name}' (ID: {tag_obj.id}) for plant {plant_id}")
            return tag_obj
        else:
            logger.warning(f"Tag '{tag_name}' not found in plant {plant_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting tag '{tag_name}' in plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

async def create_tag(session: AsyncSession, node_id: str, plant_id: str, data_source_id: int, connection_string: str = None):
    """Create a new tag in the database
    
    Args:
        session: Database session for the specific plant
        node_id (str): The node ID used as tag name
        plant_id (str): The plant ID
        data_source_id (int): The datasource ID
        connection_string (str, optional): The connection string for the tag
        
    Returns:
        Tag: The created tag object or None if failed
    """
    try:
        # Check if tag already exists
        existing_tag = await get_tag_by_name(session, node_id, plant_id)
        if existing_tag:
            logger.info(f"Tag '{node_id}' already exists with ID {existing_tag.id} in plant {plant_id}")
            return existing_tag
        
        # Use provided connection_string or fallback to node_id
        actual_connection_string = connection_string or node_id
        
        # Validate OPC UA connection string format
        if not validate_opcua_connection_string(actual_connection_string):
            logger.error(f"Invalid OPC UA connection string format: {actual_connection_string}. Expected format: ns=<namespace>;<identifier_type>=<identifier>")
            return None
        
        # Verify that the connection_string exists in the datasource
        connection_manager = get_datasource_connection_manager()
        
        try:
            # Test connection to datasource
            test_result = await connection_manager.test_connection(session, data_source_id, int(plant_id))
            if not test_result["success"]:
                logger.error(f"Datasource {data_source_id} not connected for plant {plant_id}")
                return None
                
            # Try to read the node to verify it exists using connection_string
            read_result = await connection_manager.read_node(session, data_source_id, int(plant_id), actual_connection_string)
            if read_result["success"]:
                logger.info(f"Verified connection_string {actual_connection_string} exists in datasource {data_source_id} for plant {plant_id}")
            else:
                logger.error(f"Connection_string {actual_connection_string} not found in datasource {data_source_id} for plant {plant_id}: {read_result.get('error', 'Unknown error')}")
                return None
                
        except Exception as e:
            logger.error(f"Error verifying connection_string {actual_connection_string} in datasource {data_source_id} for plant {plant_id}: {e}")
            return None
        
        # Convert plant_id to int for the plant_id field
        plant_id_int = int(plant_id) if plant_id.isdigit() else 1
        
        new_tag = Tag(
            name=node_id,
            connection_string=actual_connection_string,
            description=f"Auto-created tag for {node_id}",
            unit_of_measure="unknown",
            plant_id=plant_id_int,
            data_source_id=data_source_id,
            is_active=True
        )
        
        session.add(new_tag)
        await session.commit()
        await session.refresh(new_tag)
        
        logger.info(f"Created new tag with ID {new_tag.id} for node {node_id} with verified connection_string {actual_connection_string} in datasource {data_source_id} for plant {plant_id}")
        return new_tag
    except Exception as e:
        logger.error(f"Error creating tag for node {node_id} in plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await session.rollback()
        return None

async def update_tag(session: AsyncSession, tag_id: int, plant_id: str, **kwargs):
    """Update a tag in the database
    
    Args:
        session: Database session for the specific plant
        tag_id (int): The tag ID
        plant_id (str): The plant ID for logging
        **kwargs: Fields to update (name, description, unit_of_measure, is_active)
        
    Returns:
        Tag: The updated tag object or None if failed
    """
    try:
        # First check if the tag exists
        result = await session.execute(
            select(
                Tag.id,
                Tag.name,
                Tag.description,
                Tag.unit_of_measure,
                Tag.plant_id,
                Tag.is_active,
                Tag.created_at,
                Tag.updated_at
            ).where(Tag.id == tag_id)
        )
        tag_row = result.first()
        
        if not tag_row:
            logger.warning(f"Tag {tag_id} not found for update in plant {plant_id}")
            return None
        
        # Build dynamic UPDATE query based on provided fields
        update_fields = []
        update_params = {"tag_id": tag_id}
        
        for field, value in kwargs.items():
            if field in ["name", "connection_string", "description", "unit_of_measure", "is_active", "data_source_id"]:
                update_fields.append(f"{field} = :{field}")
                update_params[field] = value
        
        if not update_fields:
            logger.warning(f"No valid fields to update for tag {tag_id} in plant {plant_id}")
            return None
        
        # Use direct SQL UPDATE to avoid ORM column issues
        update_query = f"UPDATE tags SET {', '.join(update_fields)}, updated_at = NOW() WHERE id = :tag_id"
        await session.execute(text(update_query), update_params)
        await session.commit()
        
        # Get the updated tag
        updated_result = await session.execute(
            select(
                Tag.id,
                Tag.name,
                Tag.connection_string,
                Tag.description,
                Tag.unit_of_measure,
                Tag.plant_id,
                Tag.data_source_id,
                Tag.is_active,
                Tag.created_at,
                Tag.updated_at
            ).where(Tag.id == tag_id)
        )
        updated_row = updated_result.first()
        
        if updated_row:
            updated_tag = Tag(
                id=updated_row.id,
                name=updated_row.name,
                connection_string=updated_row.connection_string,
                description=updated_row.description,
                unit_of_measure=updated_row.unit_of_measure,
                plant_id=updated_row.plant_id,
                data_source_id=updated_row.data_source_id,
                is_active=updated_row.is_active,
                created_at=updated_row.created_at,
                updated_at=updated_row.updated_at
            )
            logger.info(f"Updated tag {tag_id} in plant {plant_id}")
            return updated_tag
        else:
            logger.error(f"Failed to retrieve updated tag {tag_id} from plant {plant_id}")
            return None
        
    except Exception as e:
        logger.error(f"Error updating tag {tag_id} in plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await session.rollback()
        return None

async def delete_tag(session: AsyncSession, tag_id: int, plant_id: str):
    """Delete a tag from the database
    
    Args:
        session: Database session for the specific plant
        tag_id (int): The tag ID
        plant_id (str): The plant ID for logging
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # First check if the tag exists
        result = await session.execute(
            select(
                Tag.id,
                Tag.name,
                Tag.connection_string,
                Tag.description,
                Tag.unit_of_measure,
                Tag.plant_id,
                Tag.data_source_id,
                Tag.is_active,
                Tag.created_at,
                Tag.updated_at
            ).where(Tag.id == tag_id)
        )
        tag_row = result.first()
        
        if not tag_row:
            logger.warning(f"Tag {tag_id} not found for deletion in plant {plant_id}")
            return False
        
        # Use direct SQL DELETE to avoid ORM column issues
        delete_result = await session.execute(
            text("DELETE FROM tags WHERE id = :tag_id"),
            {"tag_id": tag_id}
        )
        await session.commit()
        
        logger.info(f"Deleted tag {tag_id} from plant {plant_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error deleting tag {tag_id} in plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await session.rollback()
        return False

async def get_all_tags(session: AsyncSession, plant_id: str, limit: int = 100, offset: int = 0):
    """Get all tags from the plant database with pagination
    
    Args:
        session: Database session for the specific plant
        plant_id (str): The plant ID for logging
        limit (int): Maximum number of tags to return
        offset (int): Number of tags to skip
        
    Returns:
        list: List of tag objects
    """
    try:
        # Select only the columns that we know exist in the database
        result = await session.execute(
            select(
                Tag.id,
                Tag.name,
                Tag.connection_string,
                Tag.description,
                Tag.unit_of_measure,
                Tag.plant_id,
                Tag.data_source_id,
                Tag.is_active,
                Tag.created_at,
                Tag.updated_at
            )
            .limit(limit)
            .offset(offset)
        )
        tags = result.all()
        
        # Convert to Tag objects for consistency
        tag_objects = []
        for tag_row in tags:
            tag_obj = Tag(
                id=tag_row.id,
                name=tag_row.name,
                connection_string=tag_row.connection_string,
                description=tag_row.description,
                unit_of_measure=tag_row.unit_of_measure,
                plant_id=tag_row.plant_id,
                data_source_id=tag_row.data_source_id,
                is_active=tag_row.is_active,
                created_at=tag_row.created_at,
                updated_at=tag_row.updated_at
            )
            tag_objects.append(tag_obj)
        
        logger.info(f"Retrieved {len(tag_objects)} tags from plant {plant_id}")
        return tag_objects
        
    except Exception as e:
        logger.error(f"Error getting tags from plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

async def get_active_tags(session: AsyncSession, plant_id: str):
    """Get all active tags from the plant database
    
    Args:
        session: Database session for the specific plant
        plant_id (str): The plant ID for logging
        
    Returns:
        list: List of active tag objects
    """
    try:
        # Select only the columns that we know exist in the database
        result = await session.execute(
            select(
                Tag.id,
                Tag.name,
                Tag.connection_string,
                Tag.description,
                Tag.unit_of_measure,
                Tag.plant_id,
                Tag.data_source_id,
                Tag.is_active,
                Tag.created_at,
                Tag.updated_at
            )
            .where(Tag.is_active == True)
        )
        tags = result.all()
        
        # Convert to Tag objects for consistency
        tag_objects = []
        for tag_row in tags:
            tag_obj = Tag(
                id=tag_row.id,
                name=tag_row.name,
                connection_string=tag_row.connection_string,
                description=tag_row.description,
                unit_of_measure=tag_row.unit_of_measure,
                plant_id=tag_row.plant_id,
                data_source_id=tag_row.data_source_id,
                is_active=tag_row.is_active,
                created_at=tag_row.created_at,
                updated_at=tag_row.updated_at
            )
            tag_objects.append(tag_obj)
        
        logger.info(f"Retrieved {len(tag_objects)} active tags from plant {plant_id}")
        return tag_objects
        
    except Exception as e:
        logger.error(f"Error getting active tags from plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

async def get_tags_by_data_source(session: AsyncSession, data_source_id: int, plant_id: str, limit: int = 100, offset: int = 0):
    """Get all tags for a specific data source from the plant database with pagination
    
    Args:
        session: Database session for the specific plant
        data_source_id (int): The data source ID
        plant_id (str): The plant ID for logging
        limit (int): Maximum number of tags to return
        offset (int): Number of tags to skip
        
    Returns:
        list: List of tag objects for the specified data source
    """
    try:
        # Select only the columns that we know exist in the database
        result = await session.execute(
            select(
                Tag.id,
                Tag.name,
                Tag.connection_string,
                Tag.description,
                Tag.unit_of_measure,
                Tag.plant_id,
                Tag.data_source_id,
                Tag.is_active,
                Tag.created_at,
                Tag.updated_at
            )
            .where(Tag.data_source_id == data_source_id)
            .limit(limit)
            .offset(offset)
        )
        tags = result.all()
        
        # Convert to Tag objects for consistency
        tag_objects = []
        for tag_row in tags:
            tag_obj = Tag(
                id=tag_row.id,
                name=tag_row.name,
                connection_string=tag_row.connection_string,
                description=tag_row.description,
                unit_of_measure=tag_row.unit_of_measure,
                plant_id=tag_row.plant_id,
                data_source_id=tag_row.data_source_id,
                is_active=tag_row.is_active,
                created_at=tag_row.created_at,
                updated_at=tag_row.updated_at
            )
            tag_objects.append(tag_obj)
        
        logger.info(f"Retrieved {len(tag_objects)} tags for data source {data_source_id} from plant {plant_id}")
        return tag_objects
        
    except Exception as e:
        logger.error(f"Error getting tags for data source {data_source_id} from plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []
    
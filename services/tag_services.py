from sqlalchemy.ext.asyncio import AsyncSession
from queries.tag_queries import (
    get_or_create_tag_id, 
    create_tag, 
    get_tag_by_id, 
    get_tag_by_name,
    update_tag,
    delete_tag,
    get_all_tags,
    get_active_tags,
    get_tags_by_data_source
)
from utils.log import setup_logger
from utils.response import success_response, fail_response

logger = setup_logger(__name__)

async def get_or_create_tag(session: AsyncSession, name: str, plant_id: str, data_source_id: int, connection_string: str = None):
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
        # Use connection_string if provided, otherwise use name
        actual_connection_string = connection_string if connection_string else name
        tag_id = await get_or_create_tag_id(session, name, plant_id, data_source_id, actual_connection_string)
        if tag_id:
            logger.info(f"Successfully got or created tag for name {name} with connection_string {actual_connection_string} in datasource {data_source_id} for plant {plant_id}")
            return tag_id
        else:
            logger.error(f"Failed to get or create tag for name {name} with connection_string {actual_connection_string} in datasource {data_source_id} for plant {plant_id}")
            return None
    except Exception as e:
        logger.error(f"Error in get_or_create_tag service for name {name} with connection_string {connection_string} in datasource {data_source_id} for plant {plant_id}: {e}")
        return None

async def create_tag_service(session: AsyncSession, name: str, plant_id: str, data_source_id: int, connection_string: str = None):
    """Create a new tag in the database
    
    Args:
        session: Database session for the specific plant
        name (str): The tag name in the database
        plant_id (str): The plant ID
        data_source_id (int): The datasource ID
        connection_string (str): The connection string (node ID) to search in datasource
        
    Returns:
        dict: Response with tag data or error
    """
    try:
        # Use connection_string if provided, otherwise use name
        actual_connection_string = connection_string if connection_string else name
        
        # Create tag data dictionary
        tag_data = {
            'name': name,
            'connection_string': actual_connection_string,
            'data_source_id': data_source_id,
            'description': f"Auto-created tag for {name}",
            'unit_of_measure': 'unknown'
        }
        
        # Import the create_tag function from queries
        from queries.tag_queries import create_tag as create_tag_query
        
        tag = await create_tag_query(session, name, plant_id, data_source_id, actual_connection_string)  # Pass the actual connection_string
        if tag:
            return success_response(
                data={
                    "id": tag.id,
                    "name": tag.name,
                    "connection_string": tag.connection_string,
                    "description": tag.description,
                    "unit_of_measure": tag.unit_of_measure,
                    "plant_id": tag.plant_id,
                    "data_source_id": tag.data_source_id,
                    "is_active": tag.is_active
                },
                message=f"Successfully created tag '{name}' with verified connection_string '{actual_connection_string}' in datasource {data_source_id} for plant {plant_id}"
            )
        else:
            return fail_response(
                message=f"Failed to create tag '{name}' - connection_string '{actual_connection_string}' not found in datasource {data_source_id} for plant {plant_id}",
                data={"name": name, "connection_string": actual_connection_string, "plant_id": plant_id, "data_source_id": data_source_id}
            )
    except Exception as e:
        logger.error(f"Error in create_tag_service for name {name} with connection_string {connection_string} in datasource {data_source_id} for plant {plant_id}: {e}")
        return fail_response(
            message=f"Error creating tag: {str(e)}",
            data={"name": name, "connection_string": connection_string, "plant_id": plant_id, "data_source_id": data_source_id}
        )

async def get_tag_by_id_service(session: AsyncSession, tag_id: int, plant_id: str):
    """Get tag by ID from the plant database
    
    Args:
        session: Database session for the specific plant
        tag_id (int): The tag ID
        plant_id (str): The plant ID
        
    Returns:
        dict: Response with tag data or error
    """
    try:
        tag = await get_tag_by_id(session, tag_id, plant_id)
        if tag:
            return success_response(
                data={
                    "id": tag.id,
                    "name": tag.name,
                    "connection_string": tag.connection_string,
                    "description": tag.description,
                    "unit_of_measure": tag.unit_of_measure,
                    "plant_id": tag.plant_id,
                    "data_source_id": tag.data_source_id,
                    "is_active": tag.is_active
                },
                message=f"Successfully retrieved tag {tag_id} from plant {plant_id}"
            )
        else:
            return fail_response(
                message=f"Tag {tag_id} not found in plant {plant_id}",
                data={"tag_id": tag_id, "plant_id": plant_id}
            )
    except Exception as e:
        logger.error(f"Error in get_tag_by_id_service for tag {tag_id} in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error retrieving tag: {str(e)}",
            data={"tag_id": tag_id, "plant_id": plant_id}
        )

async def get_tag_by_name_service(session: AsyncSession, tag_name: str, plant_id: str):
    """Get tag by name from the plant database
    
    Args:
        session: Database session for the specific plant
        tag_name (str): The tag name
        plant_id (str): The plant ID
        
    Returns:
        dict: Response with tag data or error
    """
    try:
        tag = await get_tag_by_name(session, tag_name, plant_id)
        if tag:
            return success_response(
                data={
                    "id": tag.id,
                    "name": tag.name,
                    "connection_string": tag.connection_string,
                    "description": tag.description,
                    "unit_of_measure": tag.unit_of_measure,
                    "plant_id": tag.plant_id,
                    "data_source_id": tag.data_source_id,
                    "is_active": tag.is_active
                },
                message=f"Successfully retrieved tag '{tag_name}' from plant {plant_id}"
            )
        else:
            return fail_response(
                message=f"Tag '{tag_name}' not found in plant {plant_id}",
                data={"tag_name": tag_name, "plant_id": plant_id}
            )
    except Exception as e:
        logger.error(f"Error in get_tag_by_name_service for tag '{tag_name}' in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error retrieving tag: {str(e)}",
            data={"tag_name": tag_name, "plant_id": plant_id}
        )

async def update_tag_service(session: AsyncSession, tag_id: int, plant_id: str, **kwargs):
    """Update a tag in the database
    
    Args:
        session: Database session for the specific plant
        tag_id (int): The tag ID
        plant_id (str): The plant ID
        **kwargs: Fields to update (name, description, unit_of_measure, is_active)
        
    Returns:
        dict: Response with updated tag data or error
    """
    try:
        tag = await update_tag(session, tag_id, plant_id, **kwargs)
        if tag:
            return success_response(
                data={
                    "id": tag.id,
                    "name": tag.name,
                    "connection_string": tag.connection_string,
                    "description": tag.description,
                    "unit_of_measure": tag.unit_of_measure,
                    "plant_id": tag.plant_id,
                    "data_source_id": tag.data_source_id,
                    "is_active": tag.is_active
                },
                message=f"Successfully updated tag {tag_id} in plant {plant_id}"
            )
        else:
            return fail_response(
                message=f"Failed to update tag {tag_id} in plant {plant_id}",
                data={"tag_id": tag_id, "plant_id": plant_id, "updates": kwargs}
            )
    except Exception as e:
        logger.error(f"Error in update_tag_service for tag {tag_id} in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error updating tag: {str(e)}",
            data={"tag_id": tag_id, "plant_id": plant_id, "updates": kwargs}
        )

async def delete_tag_service(session: AsyncSession, tag_id: int, plant_id: str):
    """Delete a tag from the database
    
    Args:
        session: Database session for the specific plant
        tag_id (int): The tag ID
        plant_id (str): The plant ID
        
    Returns:
        dict: Response with success or error
    """
    try:
        success = await delete_tag(session, tag_id, plant_id)
        if success:
            return success_response(
                data={"tag_id": tag_id, "plant_id": plant_id},
                message=f"Successfully deleted tag {tag_id} from plant {plant_id}"
            )
        else:
            return fail_response(
                message=f"Failed to delete tag {tag_id} from plant {plant_id}",
                data={"tag_id": tag_id, "plant_id": plant_id}
            )
    except Exception as e:
        logger.error(f"Error in delete_tag_service for tag {tag_id} in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error deleting tag: {str(e)}",
            data={"tag_id": tag_id, "plant_id": plant_id}
        )

async def get_all_tags_service(session: AsyncSession, plant_id: str, limit: int = 100, offset: int = 0):
    """Get all tags from the plant database with pagination
    
    Args:
        session: Database session for the specific plant
        plant_id (str): The plant ID
        limit (int): Maximum number of tags to return
        offset (int): Number of tags to skip
        
    Returns:
        dict: Response with tags data or error
    """
    try:
        tags = await get_all_tags(session, plant_id, limit, offset)
        tag_data = [
            {
                "id": tag.id,
                "name": tag.name,
                "connection_string": tag.connection_string,
                "description": tag.description,
                "unit_of_measure": tag.unit_of_measure,
                "plant_id": tag.plant_id,
                "data_source_id": tag.data_source_id,
                "is_active": tag.is_active
            }
            for tag in tags
        ]
        
        return success_response(
            data={
                "tags": tag_data,
                "count": len(tag_data),
                "limit": limit,
                "offset": offset,
                "plant_id": plant_id
            },
            message=f"Successfully retrieved {len(tag_data)} tags from plant {plant_id}"
        )
    except Exception as e:
        logger.error(f"Error in get_all_tags_service for plant {plant_id}: {e}")
        return fail_response(
            message=f"Error retrieving tags: {str(e)}",
            data={"plant_id": plant_id, "limit": limit, "offset": offset}
        )

async def get_active_tags_service(session: AsyncSession, plant_id: str):
    """Get all active tags from the plant database
    
    Args:
        session: Database session for the specific plant
        plant_id (str): The plant ID
        
    Returns:
        dict: Response with active tags data or error
    """
    try:
        tags = await get_active_tags(session, plant_id)
        tag_data = [
            {
                "id": tag.id,
                "name": tag.name,
                "connection_string": tag.connection_string,
                "description": tag.description,
                "unit_of_measure": tag.unit_of_measure,
                "plant_id": tag.plant_id,
                "data_source_id": tag.data_source_id,
                "is_active": tag.is_active
            }
            for tag in tags
        ]
        
        return success_response(
            data={
                "tags": tag_data,
                "count": len(tag_data),
                "plant_id": plant_id
            },
            message=f"Successfully retrieved {len(tag_data)} active tags from plant {plant_id}"
        )
    except Exception as e:
        logger.error(f"Error in get_active_tags_service for plant {plant_id}: {e}")
        return fail_response(
            message=f"Error retrieving active tags: {str(e)}",
            data={"plant_id": plant_id}
        )

async def create_tag_with_connection_string(session: AsyncSession, tag_data: dict, plant_id: str, user_id: int):
    """Create a new tag with connection string in the specified plant database
    
    Args:
        session: Database session for the specific plant
        tag_data: Tag data dictionary containing name, connection_string, data_source_id, etc.
        plant_id: Plant ID
        user_id: User ID creating the tag
        
    Returns:
        dict: Created tag data or error response
    """
    try:
        # Validate required fields
        name = tag_data.get('name')
        connection_string = tag_data.get('connection_string')
        data_source_id = tag_data.get('data_source_id')
        
        if not name:
            return {"success": False, "error": "name is required"}
            
        if not connection_string:
            return {"success": False, "error": "connection_string is required"}
            
        if not data_source_id:
            return {"success": False, "error": "data_source_id is required"}
            
        # Import datasource query function
        from queries.datasource_queries import get_datasource_by_id
        
        # Check if datasource exists
        datasource = await get_datasource_by_id(session, data_source_id)
        if not datasource:
            return {"success": False, "error": f"Datasource {data_source_id} not found"}
            
        # Check if datasource is accessible for this plant
        if datasource.plant_id != int(plant_id):
            return {"success": False, "error": f"Datasource {data_source_id} is not accessible for plant {plant_id}"}
        
        # Get or create tag ID
        tag_id = await get_or_create_tag_id(session, name, plant_id, data_source_id, connection_string)
        if not tag_id:
            return {"success": False, "error": f"Failed to create tag for name {name} with connection_string {connection_string}"}
        
        # Get the created tag
        tag = await get_tag_by_id(session, tag_id, plant_id)
        if not tag:
            return {"success": False, "error": "Failed to retrieve created tag"}
        
        return {
            "success": True,
            "tag": {
                "id": tag.id,
                "name": tag.name,
                "connection_string": tag.connection_string,
                "description": tag.description,
                "unit_of_measure": tag.unit_of_measure,
                "plant_id": tag.plant_id,
                "data_source_id": tag.data_source_id,
                "is_active": tag.is_active,
                "created_at": tag.created_at.isoformat() if tag.created_at else None,
                "updated_at": tag.updated_at.isoformat() if tag.updated_at else None
            }
        }
        
    except Exception as e:
        logger.error(f"Error creating tag with connection string: {e}")
        return {"success": False, "error": str(e)}

async def get_tags_by_data_source_service(session: AsyncSession, data_source_id: int, plant_id: str, limit: int = 100, offset: int = 0):
    """Get all tags for a specific data source from the plant database with pagination
    
    Args:
        session: Database session for the specific plant
        data_source_id (int): The data source ID
        plant_id (str): The plant ID
        limit (int): Maximum number of tags to return
        offset (int): Number of tags to skip
        
    Returns:
        dict: Response with tags data or error
    """
    try:
        tags = await get_tags_by_data_source(session, data_source_id, plant_id, limit, offset)
        tag_data = [
            {
                "id": tag.id,
                "name": tag.name,
                "connection_string": tag.connection_string,
                "description": tag.description,
                "unit_of_measure": tag.unit_of_measure,
                "plant_id": tag.plant_id,
                "data_source_id": tag.data_source_id,
                "is_active": tag.is_active,
                "created_at": tag.created_at.isoformat() if tag.created_at else None,
                "updated_at": tag.updated_at.isoformat() if tag.updated_at else None
            }
            for tag in tags
        ]
        
        return success_response(
            data={
                "tags": tag_data,
                "count": len(tag_data),
                "limit": limit,
                "offset": offset,
                "data_source_id": data_source_id,
                "plant_id": plant_id
            },
            message=f"Successfully retrieved {len(tag_data)} tags for data source {data_source_id} from plant {plant_id}"
        )
    except Exception as e:
        logger.error(f"Error in get_tags_by_data_source_service for data source {data_source_id} in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error retrieving tags for data source: {str(e)}",
            data={"data_source_id": data_source_id, "plant_id": plant_id, "limit": limit, "offset": offset}
        )

from sqlalchemy.ext.asyncio import AsyncSession
from queries.datasource_queries import (
    # DataSource Type queries
    create_data_source_type,
    get_data_source_type_by_id,
    get_data_source_type_by_name,
    get_all_data_source_types,
    update_data_source_type,
    delete_data_source_type,
    # DataSource queries
    create_data_source,
    get_data_source_by_id,
    get_data_source_by_name,
    get_all_data_sources,
    get_active_data_sources,
    update_data_source,
    delete_data_source,
    get_data_sources_by_type,
    check_data_source_exists
)
from utils.log import setup_logger
from utils.response import success_response, fail_response
from services.datasource_connection_manager import DataSourceConnectionManager
from typing import Dict, Any, List, Optional

logger = setup_logger(__name__)

# =============================================================================
# DATA SOURCE TYPE SERVICES
# =============================================================================

async def create_data_source_type_service(
    session: AsyncSession, 
    name: str, 
    description: str = None
) -> Dict[str, Any]:
    """Create a new data source type"""
    try:
        # Check if type already exists
        existing_type = await get_data_source_type_by_name(session, name)
        if existing_type:
            return fail_response(
                message=f"Data source type '{name}' already exists",
                data={"name": name}
            )
        
        data_source_type = await create_data_source_type(session, name, description)
        if data_source_type:
            return success_response(
                data={
                    "id": data_source_type.id,
                    "name": data_source_type.name,
                    "description": data_source_type.description,
                    "is_active": data_source_type.is_active,
                    "created_at": data_source_type.created_at,
                    "updated_at": data_source_type.updated_at
                },
                message=f"Successfully created data source type '{name}'"
            )
        else:
            return fail_response(
                message=f"Failed to create data source type '{name}'",
                data={"name": name, "description": description}
            )
    except Exception as e:
        logger.error(f"Error in create_data_source_type_service for '{name}': {e}")
        return fail_response(
            message=f"Error creating data source type: {str(e)}",
            data={"name": name, "description": description}
        )

async def get_data_source_type_by_id_service(
    session: AsyncSession, 
    type_id: int
) -> Dict[str, Any]:
    """Get data source type by ID"""
    try:
        data_source_type = await get_data_source_type_by_id(session, type_id)
        if data_source_type:
            return success_response(
                data={
                    "id": data_source_type.id,
                    "name": data_source_type.name,
                    "description": data_source_type.description,
                    "is_active": data_source_type.is_active,
                    "created_at": data_source_type.created_at,
                    "updated_at": data_source_type.updated_at
                },
                message=f"Successfully retrieved data source type {type_id}"
            )
        else:
            return fail_response(
                message=f"Data source type {type_id} not found",
                data={"type_id": type_id}
            )
    except Exception as e:
        logger.error(f"Error in get_data_source_type_by_id_service for {type_id}: {e}")
        return fail_response(
            message=f"Error retrieving data source type: {str(e)}",
            data={"type_id": type_id}
        )

async def get_data_source_type_by_name_service(
    session: AsyncSession, 
    name: str
) -> Dict[str, Any]:
    """Get data source type by name"""
    try:
        data_source_type = await get_data_source_type_by_name(session, name)
        if data_source_type:
            return success_response(
                data={
                    "id": data_source_type.id,
                    "name": data_source_type.name,
                    "description": data_source_type.description,
                    "is_active": data_source_type.is_active,
                    "created_at": data_source_type.created_at,
                    "updated_at": data_source_type.updated_at
                },
                message=f"Successfully retrieved data source type '{name}'"
            )
        else:
            return fail_response(
                message=f"Data source type '{name}' not found",
                data={"name": name}
            )
    except Exception as e:
        logger.error(f"Error in get_data_source_type_by_name_service for '{name}': {e}")
        return fail_response(
            message=f"Error retrieving data source type: {str(e)}",
            data={"name": name}
        )

async def get_all_data_source_types_service(
    session: AsyncSession, 
    active_only: bool = True
) -> Dict[str, Any]:
    """Get all data source types"""
    try:
        data_source_types = await get_all_data_source_types(session, active_only)
        types_data = []
        for ds_type in data_source_types:
            types_data.append({
                "id": ds_type.id,
                "name": ds_type.name,
                "description": ds_type.description,
                "is_active": ds_type.is_active,
                "created_at": ds_type.created_at,
                "updated_at": ds_type.updated_at
            })
        
        return success_response(
            data=types_data,
            message=f"Successfully retrieved {len(types_data)} data source types"
        )
    except Exception as e:
        logger.error(f"Error in get_all_data_source_types_service: {e}")
        return fail_response(
            message=f"Error retrieving data source types: {str(e)}"
        )

async def update_data_source_type_service(
    session: AsyncSession, 
    type_id: int, 
    **kwargs
) -> Dict[str, Any]:
    """Update a data source type"""
    try:
        # Check if type exists
        existing_type = await get_data_source_type_by_id(session, type_id)
        if not existing_type:
            return fail_response(
                message=f"Data source type {type_id} not found",
                data={"type_id": type_id}
            )
        
        # If name is being updated, check for duplicates
        if 'name' in kwargs:
            existing_name = await get_data_source_type_by_name(session, kwargs['name'])
            if existing_name and existing_name.id != type_id:
                return fail_response(
                    message=f"Data source type '{kwargs['name']}' already exists",
                    data={"type_id": type_id, "name": kwargs['name']}
                )
        
        updated_type = await update_data_source_type(session, type_id, **kwargs)
        if updated_type:
            return success_response(
                data={
                    "id": updated_type.id,
                    "name": updated_type.name,
                    "description": updated_type.description,
                    "is_active": updated_type.is_active,
                    "created_at": updated_type.created_at,
                    "updated_at": updated_type.updated_at
                },
                message=f"Successfully updated data source type {type_id}"
            )
        else:
            return fail_response(
                message=f"Failed to update data source type {type_id}",
                data={"type_id": type_id, "updates": kwargs}
            )
    except Exception as e:
        logger.error(f"Error in update_data_source_type_service for {type_id}: {e}")
        return fail_response(
            message=f"Error updating data source type: {str(e)}",
            data={"type_id": type_id, "updates": kwargs}
        )

async def delete_data_source_type_service(
    session: AsyncSession, 
    type_id: int
) -> Dict[str, Any]:
    """Delete a data source type (soft delete)"""
    try:
        # Check if type exists
        existing_type = await get_data_source_type_by_id(session, type_id)
        if not existing_type:
            return fail_response(
                message=f"Data source type {type_id} not found",
                data={"type_id": type_id}
            )
        
        success = await delete_data_source_type(session, type_id)
        if success:
            return success_response(
                data={"type_id": type_id},
                message=f"Successfully deleted data source type {type_id}"
            )
        else:
            return fail_response(
                message=f"Failed to delete data source type {type_id}",
                data={"type_id": type_id}
            )
    except Exception as e:
        logger.error(f"Error in delete_data_source_type_service for {type_id}: {e}")
        return fail_response(
            message=f"Error deleting data source type: {str(e)}",
            data={"type_id": type_id}
        )

# =============================================================================
# DATA SOURCE SERVICES
# =============================================================================

async def create_data_source_service(
    session: AsyncSession,
    name: str,
    type_id: int,
    plant_id: int,
    description: str = None,
    connection_config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Create a new data source"""
    try:
        # Check if data source already exists
        exists = await check_data_source_exists(session, name, plant_id)
        if exists:
            return fail_response(
                message=f"Data source '{name}' already exists in plant {plant_id}",
                data={"name": name, "plant_id": plant_id}
            )
        
        # Verify data source type exists
        data_source_type = await get_data_source_type_by_id(session, type_id)
        if not data_source_type:
            return fail_response(
                message=f"Data source type {type_id} not found",
                data={"type_id": type_id, "name": name, "plant_id": plant_id}
            )
        
        data_source = await create_data_source(
            session, name, type_id, plant_id, description, connection_config
        )
        if data_source:
            return success_response(
                data={
                    "id": data_source.id,
                    "name": data_source.name,
                    "description": data_source.description,
                    "type_id": data_source.type_id,
                    "type_name": data_source.data_source_type.name,
                    "plant_id": data_source.plant_id,
                    "connection_config": data_source.connection_config,
                    "is_active": data_source.is_active,
                    "created_at": data_source.created_at,
                    "updated_at": data_source.updated_at
                },
                message=f"Successfully created data source '{name}' in plant {plant_id}"
            )
        else:
            return fail_response(
                message=f"Failed to create data source '{name}' in plant {plant_id}",
                data={"name": name, "type_id": type_id, "plant_id": plant_id}
            )
    except Exception as e:
        logger.error(f"Error in create_data_source_service for '{name}' in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error creating data source: {str(e)}",
            data={"name": name, "type_id": type_id, "plant_id": plant_id}
        )

async def get_data_source_by_id_service(
    session: AsyncSession,
    source_id: int,
    plant_id: int
) -> Dict[str, Any]:
    """Get data source by ID and plant ID"""
    try:
        data_source = await get_data_source_by_id(session, source_id, plant_id)
        if data_source:
            return success_response(
                data={
                    "id": data_source.id,
                    "name": data_source.name,
                    "description": data_source.description,
                    "type_id": data_source.type_id,
                    "type_name": data_source.data_source_type.name,
                    "plant_id": data_source.plant_id,
                    "connection_config": data_source.connection_config,
                    "is_active": data_source.is_active,
                    "created_at": data_source.created_at,
                    "updated_at": data_source.updated_at
                },
                message=f"Successfully retrieved data source {source_id} from plant {plant_id}"
            )
        else:
            return fail_response(
                message=f"Data source {source_id} not found in plant {plant_id}",
                data={"source_id": source_id, "plant_id": plant_id}
            )
    except Exception as e:
        logger.error(f"Error in get_data_source_by_id_service for {source_id} in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error retrieving data source: {str(e)}",
            data={"source_id": source_id, "plant_id": plant_id}
        )

async def get_data_source_by_name_service(
    session: AsyncSession,
    name: str,
    plant_id: int
) -> Dict[str, Any]:
    """Get data source by name and plant ID"""
    try:
        data_source = await get_data_source_by_name(session, name, plant_id)
        if data_source:
            return success_response(
                data={
                    "id": data_source.id,
                    "name": data_source.name,
                    "description": data_source.description,
                    "type_id": data_source.type_id,
                    "type_name": data_source.data_source_type.name,
                    "plant_id": data_source.plant_id,
                    "connection_config": data_source.connection_config,
                    "is_active": data_source.is_active,
                    "created_at": data_source.created_at,
                    "updated_at": data_source.updated_at
                },
                message=f"Successfully retrieved data source '{name}' from plant {plant_id}"
            )
        else:
            return fail_response(
                message=f"Data source '{name}' not found in plant {plant_id}",
                data={"name": name, "plant_id": plant_id}
            )
    except Exception as e:
        logger.error(f"Error in get_data_source_by_name_service for '{name}' in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error retrieving data source: {str(e)}",
            data={"name": name, "plant_id": plant_id}
        )

async def get_all_data_sources_service(
    session: AsyncSession,
    plant_id: int,
    active_only: bool = True,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "name",
    sort_direction: str = "asc"
) -> Dict[str, Any]:
    """Get all data sources for a plant"""
    try:
        data_sources = await get_all_data_sources(session, plant_id, active_only, limit, offset, sort_by, sort_direction)
        sources_data = []
        for ds in data_sources:
            sources_data.append({
                "id": ds.id,
                "name": ds.name,
                "description": ds.description,
                "type_id": ds.type_id,
                "type_name": ds.data_source_type.name,
                "plant_id": ds.plant_id,
                "connection_config": ds.connection_config,
                "is_active": ds.is_active,
                "created_at": ds.created_at,
                "updated_at": ds.updated_at
            })
        
        return success_response(
            data=sources_data,
            message=f"Successfully retrieved {len(sources_data)} data sources from plant {plant_id}"
        )
    except Exception as e:
        logger.error(f"Error in get_all_data_sources_service for plant {plant_id}: {e}")
        return fail_response(
            message=f"Error retrieving data sources: {str(e)}",
            data={"plant_id": plant_id}
        )

async def get_active_data_sources_service(
    session: AsyncSession,
    plant_id: int
) -> Dict[str, Any]:
    """Get all active data sources for a plant"""
    try:
        data_sources = await get_active_data_sources(session, plant_id)
        sources_data = []
        for ds in data_sources:
            sources_data.append({
                "id": ds.id,
                "name": ds.name,
                "description": ds.description,
                "type_id": ds.type_id,
                "type_name": ds.data_source_type.name,
                "plant_id": ds.plant_id,
                "connection_config": ds.connection_config,
                "is_active": ds.is_active,
                "created_at": ds.created_at,
                "updated_at": ds.updated_at
            })
        
        return success_response(
            data=sources_data,
            message=f"Successfully retrieved {len(sources_data)} active data sources from plant {plant_id}"
        )
    except Exception as e:
        logger.error(f"Error in get_active_data_sources_service for plant {plant_id}: {e}")
        return fail_response(
            message=f"Error retrieving active data sources: {str(e)}",
            data={"plant_id": plant_id}
        )

async def update_data_source_service(
    session: AsyncSession,
    source_id: int,
    plant_id: int,
    **kwargs
) -> Dict[str, Any]:
    """Update a data source"""
    try:
        # Check if data source exists
        existing_source = await get_data_source_by_id(session, source_id, plant_id)
        if not existing_source:
            return fail_response(
                message=f"Data source {source_id} not found in plant {plant_id}",
                data={"source_id": source_id, "plant_id": plant_id}
            )
        
        # If name is being updated, check for duplicates
        if 'name' in kwargs:
            exists = await check_data_source_exists(session, kwargs['name'], plant_id)
            if exists:
                # Check if it's the same data source
                existing_by_name = await get_data_source_by_name(session, kwargs['name'], plant_id)
                if existing_by_name and existing_by_name.id != source_id:
                    return fail_response(
                        message=f"Data source '{kwargs['name']}' already exists in plant {plant_id}",
                        data={"source_id": source_id, "plant_id": plant_id, "name": kwargs['name']}
                    )
        
        # If type_id is being updated, verify the type exists
        if 'type_id' in kwargs:
            data_source_type = await get_data_source_type_by_id(session, kwargs['type_id'])
            if not data_source_type:
                return fail_response(
                    message=f"Data source type {kwargs['type_id']} not found",
                    data={"source_id": source_id, "plant_id": plant_id, "type_id": kwargs['type_id']}
                )
        
        updated_source = await update_data_source(session, source_id, plant_id, **kwargs)
        if updated_source:
            return success_response(
                data={
                    "id": updated_source.id,
                    "name": updated_source.name,
                    "description": updated_source.description,
                    "type_id": updated_source.type_id,
                    "type_name": updated_source.data_source_type.name,
                    "plant_id": updated_source.plant_id,
                    "connection_config": updated_source.connection_config,
                    "is_active": updated_source.is_active,
                    "created_at": updated_source.created_at,
                    "updated_at": updated_source.updated_at
                },
                message=f"Successfully updated data source {source_id} in plant {plant_id}"
            )
        else:
            return fail_response(
                message=f"Failed to update data source {source_id} in plant {plant_id}",
                data={"source_id": source_id, "plant_id": plant_id, "updates": kwargs}
            )
    except Exception as e:
        logger.error(f"Error in update_data_source_service for {source_id} in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error updating data source: {str(e)}",
            data={"source_id": source_id, "plant_id": plant_id, "updates": kwargs}
        )

async def delete_data_source_service(
    session: AsyncSession,
    source_id: int,
    plant_id: int
) -> Dict[str, Any]:
    """Delete a data source (soft delete)"""
    try:
        # Check if data source exists
        existing_source = await get_data_source_by_id(session, source_id, plant_id)
        if not existing_source:
            return fail_response(
                message=f"Data source {source_id} not found in plant {plant_id}",
                data={"source_id": source_id, "plant_id": plant_id}
            )
        
        success = await delete_data_source(session, source_id, plant_id)
        if success:
            return success_response(
                data={"source_id": source_id, "plant_id": plant_id},
                message=f"Successfully deleted data source {source_id} from plant {plant_id}"
            )
        else:
            return fail_response(
                message=f"Failed to delete data source {source_id} from plant {plant_id}",
                data={"source_id": source_id, "plant_id": plant_id}
            )
    except Exception as e:
        logger.error(f"Error in delete_data_source_service for {source_id} in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error deleting data source: {str(e)}",
            data={"source_id": source_id, "plant_id": plant_id}
        )

async def get_data_sources_by_type_service(
    session: AsyncSession,
    type_id: int,
    plant_id: int,
    active_only: bool = True
) -> Dict[str, Any]:
    """Get data sources by type for a specific plant"""
    try:
        # Verify data source type exists
        data_source_type = await get_data_source_type_by_id(session, type_id)
        if not data_source_type:
            return fail_response(
                message=f"Data source type {type_id} not found",
                data={"type_id": type_id, "plant_id": plant_id}
            )
        
        data_sources = await get_data_sources_by_type(session, type_id, plant_id, active_only)
        sources_data = []
        for ds in data_sources:
            sources_data.append({
                "id": ds.id,
                "name": ds.name,
                "description": ds.description,
                "type_id": ds.type_id,
                "type_name": ds.data_source_type.name,
                "plant_id": ds.plant_id,
                "connection_config": ds.connection_config,
                "is_active": ds.is_active,
                "created_at": ds.created_at,
                "updated_at": ds.updated_at
            })
        
        return success_response(
            data=sources_data,
            message=f"Successfully retrieved {len(sources_data)} data sources of type {data_source_type.name} from plant {plant_id}"
        )
    except Exception as e:
        logger.error(f"Error in get_data_sources_by_type_service for type {type_id} in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error retrieving data sources by type: {str(e)}",
            data={"type_id": type_id, "plant_id": plant_id}
        )

async def test_data_source_connection_service(
    session: AsyncSession,
    source_id: int,
    plant_id: int
) -> Dict[str, Any]:
    """Test data source connection"""
    try:
        # Check if data source exists
        existing_source = await get_data_source_by_id(session, source_id, plant_id)
        if not existing_source:
            return fail_response(
                message=f"Data source {source_id} not found in plant {plant_id}",
                data={"source_id": source_id, "plant_id": plant_id}
            )
        
        # Test connection
        connection_manager = DataSourceConnectionManager()
        success = await connection_manager.test_connection_object(existing_source)
        if success:
            return success_response(
                data={"source_id": source_id, "plant_id": plant_id},
                message=f"Successfully tested data source connection for {source_id} in plant {plant_id}"
            )
        else:
            return fail_response(
                message=f"Failed to test data source connection for {source_id} in plant {plant_id}",
                data={"source_id": source_id, "plant_id": plant_id}
            )
    except Exception as e:
        logger.error(f"Error in test_data_source_connection_service for {source_id} in plant {plant_id}: {e}")
        return fail_response(
            message=f"Error testing data source connection: {str(e)}",
            data={"source_id": source_id, "plant_id": plant_id}
        )

async def test_data_source_connection_config_service(
    session: AsyncSession,
    type_id: int,
    connection_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Test data source connection configuration without saving to database"""
    try:
        # Verify data source type exists
        data_source_type = await get_data_source_type_by_id(session, type_id)
        if not data_source_type:
            return fail_response(
                message=f"Data source type {type_id} not found",
                data={"type_id": type_id}
            )
        
        # Create a temporary data source object for testing
        from models.plant_models import DataSource
        temp_datasource = DataSource(
            id=0,  # Temporary ID
            name="test_connection",
            type_id=type_id,
            plant_id=0,  # Temporary plant ID
            description="Temporary datasource for connection testing",
            connection_config=connection_config,
            is_active=True
        )
        
        # Manually set the data_source_type relationship
        temp_datasource.data_source_type = data_source_type
        
        # Test connection
        connection_manager = DataSourceConnectionManager()
        success = await connection_manager.test_connection_object(temp_datasource)
        
        if success:
            return success_response(
                data={
                    "type_id": type_id,
                    "type_name": data_source_type.name,
                    "connection_config": connection_config
                },
                message=f"Successfully tested connection for {data_source_type.name} data source"
            )
        else:
            return fail_response(
                message=f"Failed to test connection for {data_source_type.name} data source",
                data={
                    "type_id": type_id,
                    "type_name": data_source_type.name,
                    "connection_config": connection_config
                }
            )
    except Exception as e:
        logger.error(f"Error in test_data_source_connection_config_service for type {type_id}: {e}")
        return fail_response(
            message=f"Error testing data source connection: {str(e)}",
            data={"type_id": type_id, "connection_config": connection_config}
        ) 
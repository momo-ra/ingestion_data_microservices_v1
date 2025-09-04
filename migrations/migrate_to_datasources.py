"""
Migration script to set up datasource types and migrate existing configurations
"""

import asyncio
import json
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_plant_db, get_central_db
from queries.datasource_queries import (
    create_data_source_type,
    get_data_source_type_by_name,
    create_data_source,
    get_data_source_by_name,
    get_all_data_sources
)
from queries.tag_queries import get_all_tags, update_tag
from config.settings import settings
from utils.log import setup_logger

logger = setup_logger(__name__)

async def create_default_datasource_types(session: AsyncSession):
    """Create default datasource types"""
    try:
        # Create OPC UA type
        opcua_type = await get_data_source_type_by_name(session, "opcua")
        if not opcua_type:
            opcua_type = await create_data_source_type(
                session, 
                "opcua", 
                "OPC UA server connection"
            )
            logger.info("Created OPC UA datasource type")
        else:
            logger.info("OPC UA datasource type already exists")
        
        # Create Database type
        db_type = await get_data_source_type_by_name(session, "database")
        if not db_type:
            db_type = await create_data_source_type(
                session, 
                "database", 
                "Database connection (PostgreSQL, MySQL, etc.)"
            )
            logger.info("Created Database datasource type")
        else:
            logger.info("Database datasource type already exists")
        
        # Create Modbus type
        modbus_type = await get_data_source_type_by_name(session, "modbus")
        if not modbus_type:
            modbus_type = await create_data_source_type(
                session, 
                "modbus", 
                "Modbus TCP/RTU connection"
            )
            logger.info("Created Modbus datasource type")
        else:
            logger.info("Modbus datasource type already exists")
        
        return {
            "opcua": opcua_type,
            "database": db_type,
            "modbus": modbus_type
        }
        
    except Exception as e:
        logger.error(f"Error creating default datasource types: {e}")
        raise

async def migrate_opcua_configuration(session: AsyncSession, plant_id: int):
    """Migrate existing OPC UA configuration to new datasource format"""
    try:
        # Get OPC UA type
        opcua_type = await get_data_source_type_by_name(session, "opcua")
        if not opcua_type:
            logger.error("OPC UA datasource type not found")
            return None
        
        # Create default OPC UA datasource from settings
        default_opcua_name = f"default_opcua_plant_{plant_id}"
        existing_ds = await get_data_source_by_name(session, default_opcua_name, plant_id)
        
        if existing_ds:
            logger.info(f"Default OPC UA datasource already exists for plant {plant_id}")
            return existing_ds
        
        # Create connection config from settings
        connection_config = {
            "url": settings.opcua.url,
            "connection_timeout": settings.opcua.connection_timeout,
            "max_retries": settings.opcua.max_reconnect_attempts,
            "reconnect_delay": settings.opcua.reconnect_delay,
            "connection_check_interval": settings.opcua.connection_check_interval
        }
        
        # Create the datasource
        datasource = await create_data_source(
            session=session,
            name=default_opcua_name,
            type_id=opcua_type.id,
            plant_id=plant_id,
            description=f"Default OPC UA connection for plant {plant_id}",
            connection_config=connection_config
        )
        
        if datasource:
            logger.info(f"Created default OPC UA datasource for plant {plant_id}")
            return datasource
        else:
            logger.error(f"Failed to create default OPC UA datasource for plant {plant_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error migrating OPC UA configuration for plant {plant_id}: {e}")
        return None

async def migrate_existing_tags(session: AsyncSession, plant_id: int, default_datasource_id: int):
    """Migrate existing tags to use the default datasource"""
    try:
        # Get all tags that don't have a datasource_id
        tags = await get_all_tags(session, str(plant_id), limit=1000, offset=0)
        
        migrated_count = 0
        for tag in tags:
            if not hasattr(tag, 'data_source_id') or tag.data_source_id is None:
                # Update tag to use default datasource and set connection_string
                success = await update_tag(
                    session, 
                    tag.id, 
                    str(plant_id), 
                    data_source_id=default_datasource_id,
                    connection_string=tag.name  # Use tag name as connection_string
                )
                if success:
                    migrated_count += 1
                    logger.info(f"Migrated tag {tag.id} ({tag.name}) to datasource {default_datasource_id} with connection_string {tag.name}")
                else:
                    logger.error(f"Failed to migrate tag {tag.id} ({tag.name})")
        
        logger.info(f"Migrated {migrated_count} tags to default datasource for plant {plant_id}")
        return migrated_count
        
    except Exception as e:
        logger.error(f"Error migrating tags for plant {plant_id}: {e}")
        return 0

async def create_tags_for_opc_nodes(session: AsyncSession, plant_id: int, opc_datasource_id: int):
    """Create tags for existing OPC UA nodes with connection_string"""
    try:
        from sqlalchemy import text
        from services.tag_services import create_tag_with_connection_string
        
        # Get existing OPC UA nodes from the database
        opc_nodes_query = text("""
            SELECT DISTINCT node_id, plant_id 
            FROM opc_ua_nodes 
            WHERE is_active = true AND plant_id = :plant_id
        """)
        opc_nodes_result = await session.execute(opc_nodes_query, {"plant_id": plant_id})
        opc_nodes = opc_nodes_result.fetchall()
        
        created_count = 0
        for node in opc_nodes:
            node_id = node[0]
            
            # Create tag for this node with connection_string
            tag_data = {
                'name': node_id,
                'connection_string': node_id,  # Use node_id as connection_string for OPC UA
                'data_source_id': opc_datasource_id,
                'description': f'Migrated tag for {node_id}',
                'unit_of_measure': 'unknown'
            }
            
            try:
                result = await create_tag_with_connection_string(session, tag_data, str(plant_id), 0)
                if result.get('success'):
                    created_count += 1
                    logger.info(f"Created tag for name {node_id} with connection_string {node_id} in plant {plant_id}")
                else:
                    logger.warning(f"Failed to create tag for name {node_id} in plant {plant_id}: {result.get('error')}")
            except Exception as e:
                logger.error(f"Error creating tag for name {node_id} in plant {plant_id}: {e}")
        
        logger.info(f"Created {created_count} tags for OPC UA nodes in plant {plant_id}")
        return created_count
        
    except Exception as e:
        logger.error(f"Error creating tags for OPC UA nodes in plant {plant_id}: {e}")
        return 0

async def run_migration():
    """Run the complete migration"""
    try:
        logger.info("Starting datasource migration...")
        
        # Get central database session
        async for central_session in get_central_db():
            # Create default datasource types
            logger.info("Creating default datasource types...")
            types = await create_default_datasource_types(central_session)
            
            # Get all plants (you may need to adjust this based on your plant structure)
            # For now, we'll assume plant_id = 1 as default
            plant_ids = [1]  # Add more plant IDs as needed
            
            for plant_id in plant_ids:
                logger.info(f"Migrating plant {plant_id}...")
                
                # Get plant database session
                async for plant_session in get_plant_db(str(plant_id)):
                    # Migrate OPC UA configuration
                    default_datasource = await migrate_opcua_configuration(plant_session, plant_id)
                    
                    if default_datasource:
                        # Migrate existing tags
                        await migrate_existing_tags(plant_session, plant_id, default_datasource.id)
                        
                        # Create tags for existing OPC UA nodes
                        await create_tags_for_opc_nodes(plant_session, plant_id, default_datasource.id)
                    else:
                        logger.error(f"Could not create default datasource for plant {plant_id}")
                
                logger.info(f"Completed migration for plant {plant_id}")
            
            logger.info("Datasource migration completed successfully!")
            break
            
    except Exception as e:
        logger.error(f"Error during migration: {e}")
        raise

async def verify_migration():
    """Verify that the migration was successful"""
    try:
        logger.info("Verifying migration...")
        
        # Get central database session
        async for central_session in get_central_db():
            # Check datasource types
            from queries.datasource_queries import get_all_data_source_types
            types = await get_all_data_source_types(central_session, active_only=True)
            logger.info(f"Found {len(types)} datasource types: {[t.name for t in types]}")
            
            # Check datasources for each plant
            plant_ids = [1]  # Add more plant IDs as needed
            
            for plant_id in plant_ids:
                logger.info(f"Verifying plant {plant_id}...")
                
                # Get plant database session
                async for plant_session in get_plant_db(str(plant_id)):
                    # Check datasources
                    from queries.datasource_queries import get_all_data_sources
                    datasources = await get_all_data_sources(plant_session, plant_id, active_only=True)
                    logger.info(f"Found {len(datasources)} datasources for plant {plant_id}")
                    
                    for ds in datasources:
                        logger.info(f"  - {ds.name} ({ds.data_source_type.name})")
                    
                    # Check tags
                    tags = await get_all_tags(plant_session, str(plant_id), limit=100, offset=0)
                    tags_with_datasource = [t for t in tags if hasattr(t, 'data_source_id') and t.data_source_id is not None]
                    logger.info(f"Found {len(tags_with_datasource)}/{len(tags)} tags with datasource for plant {plant_id}")
                
                logger.info(f"Verification completed for plant {plant_id}")
            
            logger.info("Migration verification completed!")
            break
            
    except Exception as e:
        logger.error(f"Error during verification: {e}")
        raise

if __name__ == "__main__":
    # Run migration
    asyncio.run(run_migration())
    
    # Verify migration
    asyncio.run(verify_migration()) 
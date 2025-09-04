from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, and_, asc, desc
from sqlalchemy.orm import selectinload
from models.plant_models import DataSource, DataSourceType
from typing import List, Optional, Dict, Any
from utils.log import setup_logger

logger = setup_logger(__name__)

# =============================================================================
# DATA SOURCE TYPE QUERIES
# =============================================================================

async def create_data_source_type(session: AsyncSession, name: str, description: str = None) -> Optional[DataSourceType]:
    """Create a new data source type"""
    try:
        data_source_type = DataSourceType(
            name=name,
            description=description
        )
        session.add(data_source_type)
        await session.commit()
        await session.refresh(data_source_type)
        logger.info(f"Created data source type: {name}")
        return data_source_type
    except Exception as e:
        await session.rollback()
        logger.error(f"Error creating data source type {name}: {e}")
        return None

async def get_data_source_type_by_id(session: AsyncSession, type_id: int) -> Optional[DataSourceType]:
    """Get data source type by ID"""
    try:
        result = await session.execute(
            select(DataSourceType).where(DataSourceType.id == type_id)
        )
        return result.scalar_one_or_none()
    except Exception as e:
        logger.error(f"Error getting data source type by ID {type_id}: {e}")
        return None

async def get_data_source_type_by_name(session: AsyncSession, name: str) -> Optional[DataSourceType]:
    """Get data source type by name"""
    try:
        result = await session.execute(
            select(DataSourceType).where(DataSourceType.name == name)
        )
        return result.scalar_one_or_none()
    except Exception as e:
        logger.error(f"Error getting data source type by name {name}: {e}")
        return None

async def get_all_data_source_types(session: AsyncSession, active_only: bool = True) -> List[DataSourceType]:
    """Get all data source types"""
    try:
        query = select(DataSourceType)
        if active_only:
            query = query.where(DataSourceType.is_active == True)
        query = query.order_by(DataSourceType.name)
        
        result = await session.execute(query)
        return result.scalars().all()
    except Exception as e:
        logger.error(f"Error getting all data source types: {e}")
        return []

async def update_data_source_type(session: AsyncSession, type_id: int, **kwargs) -> Optional[DataSourceType]:
    """Update a data source type"""
    try:
        result = await session.execute(
            update(DataSourceType)
            .where(DataSourceType.id == type_id)
            .values(**kwargs)
            .returning(DataSourceType)
        )
        updated_type = result.scalar_one_or_none()
        if updated_type:
            await session.commit()
            logger.info(f"Updated data source type {type_id}")
            return updated_type
        return None
    except Exception as e:
        await session.rollback()
        logger.error(f"Error updating data source type {type_id}: {e}")
        return None

async def delete_data_source_type(session: AsyncSession, type_id: int) -> bool:
    """Delete a data source type (soft delete by setting is_active to False)"""
    try:
        result = await session.execute(
            update(DataSourceType)
            .where(DataSourceType.id == type_id)
            .values(is_active=False)
        )
        await session.commit()
        logger.info(f"Deleted data source type {type_id}")
        return True
    except Exception as e:
        await session.rollback()
        logger.error(f"Error deleting data source type {type_id}: {e}")
        return False

# =============================================================================
# DATA SOURCE QUERIES
# =============================================================================

async def create_data_source(
    session: AsyncSession, 
    name: str, 
    type_id: int, 
    plant_id: int, 
    description: str = None,
    connection_config: Dict[str, Any] = None
) -> Optional[DataSource]:
    """Create a new data source"""
    try:
        data_source = DataSource(
            name=name,
            description=description,
            type_id=type_id,
            plant_id=plant_id,
            connection_config=connection_config
        )
        session.add(data_source)
        await session.commit()
        await session.refresh(data_source)
        logger.info(f"Created data source: {name} for plant {plant_id}")
        return data_source
    except Exception as e:
        await session.rollback()
        logger.error(f"Error creating data source {name}: {e}")
        return None

async def get_data_source_by_id(session: AsyncSession, source_id: int, plant_id: int) -> Optional[DataSource]:
    """Get data source by ID and plant ID"""
    try:
        result = await session.execute(
            select(DataSource)
            .options(selectinload(DataSource.data_source_type))
            .where(
                and_(
                    DataSource.id == source_id,
                    DataSource.plant_id == plant_id
                )
            )
        )
        return result.scalar_one_or_none()
    except Exception as e:
        logger.error(f"Error getting data source by ID {source_id}: {e}")
        return None

async def get_data_source_by_name(session: AsyncSession, name: str, plant_id: int) -> Optional[DataSource]:
    """Get data source by name and plant ID"""
    try:
        result = await session.execute(
            select(DataSource)
            .options(selectinload(DataSource.data_source_type))
            .where(
                and_(
                    DataSource.name == name,
                    DataSource.plant_id == plant_id
                )
            )
        )
        return result.scalar_one_or_none()
    except Exception as e:
        logger.error(f"Error getting data source by name {name}: {e}")
        return None

async def get_all_data_sources(
    session: AsyncSession, 
    plant_id: int, 
    active_only: bool = True,
    limit: int = 100, 
    offset: int = 0,
    sort_by: str = "name",
    sort_direction: str = "asc"
) -> List[DataSource]:
    """Get all data sources for a plant"""
    try:
        query = select(DataSource).options(selectinload(DataSource.data_source_type))
        query = query.where(DataSource.plant_id == plant_id)
        
        if active_only:
            query = query.where(DataSource.is_active == True)
        
        # Apply sorting
        sort_column = None
        if sort_by == "name":
            sort_column = DataSource.name
        elif sort_by == "created_at":
            sort_column = DataSource.created_at
        elif sort_by == "updated_at":
            sort_column = DataSource.updated_at
        elif sort_by == "type_name":
            # Since we're using selectinload(DataSource.data_source_type), 
            # the relationship should be available for sorting
            sort_column = DataSource.data_source_type.name
        else:
            # Default to name if invalid sort_by is provided
            sort_column = DataSource.name
        
        # Apply sort direction
        if sort_direction.lower() == "desc":
            query = query.order_by(desc(sort_column))
        else:
            query = query.order_by(asc(sort_column))
        
        query = query.limit(limit).offset(offset)
        
        result = await session.execute(query)
        return result.scalars().all()
    except Exception as e:
        logger.error(f"Error getting all data sources for plant {plant_id}: {e}")
        return []

async def get_active_data_sources(session: AsyncSession, plant_id: int) -> List[DataSource]:
    """Get all active data sources for a plant"""
    try:
        result = await session.execute(
            select(DataSource)
            .options(selectinload(DataSource.data_source_type))
            .where(
                and_(
                    DataSource.plant_id == plant_id,
                    DataSource.is_active == True
                )
            )
            .order_by(DataSource.name)
        )
        return result.scalars().all()
    except Exception as e:
        logger.error(f"Error getting active data sources for plant {plant_id}: {e}")
        return []

async def update_data_source(
    session: AsyncSession, 
    source_id: int, 
    plant_id: int, 
    **kwargs
) -> Optional[DataSource]:
    """Update a data source"""
    try:
        result = await session.execute(
            update(DataSource)
            .where(
                and_(
                    DataSource.id == source_id,
                    DataSource.plant_id == plant_id
                )
            )
            .values(**kwargs)
            .returning(DataSource)
        )
        updated_source = result.scalar_one_or_none()
        if updated_source:
            await session.commit()
            logger.info(f"Updated data source {source_id} in plant {plant_id}")
            return updated_source
        return None
    except Exception as e:
        await session.rollback()
        logger.error(f"Error updating data source {source_id}: {e}")
        return None

async def delete_data_source(session: AsyncSession, source_id: int, plant_id: int) -> bool:
    """Delete a data source (soft delete by setting is_active to False)"""
    try:
        result = await session.execute(
            update(DataSource)
            .where(
                and_(
                    DataSource.id == source_id,
                    DataSource.plant_id == plant_id
                )
            )
            .values(is_active=False)
        )
        await session.commit()
        logger.info(f"Deleted data source {source_id} in plant {plant_id}")
        return True
    except Exception as e:
        await session.rollback()
        logger.error(f"Error deleting data source {source_id}: {e}")
        return False

async def get_data_sources_by_type(
    session: AsyncSession, 
    type_id: int, 
    plant_id: int, 
    active_only: bool = True
) -> List[DataSource]:
    """Get data sources by type for a specific plant"""
    try:
        query = select(DataSource).options(selectinload(DataSource.data_source_type))
        query = query.where(
            and_(
                DataSource.type_id == type_id,
                DataSource.plant_id == plant_id
            )
        )
        
        if active_only:
            query = query.where(DataSource.is_active == True)
        
        query = query.order_by(DataSource.name)
        
        result = await session.execute(query)
        return result.scalars().all()
    except Exception as e:
        logger.error(f"Error getting data sources by type {type_id} for plant {plant_id}: {e}")
        return []

async def check_data_source_exists(session: AsyncSession, name: str, plant_id: int) -> bool:
    """Check if a data source exists by name and plant ID"""
    try:
        result = await session.execute(
            select(DataSource.id)
            .where(
                and_(
                    DataSource.name == name,
                    DataSource.plant_id == plant_id
                )
            )
        )
        return result.scalar_one_or_none() is not None
    except Exception as e:
        logger.error(f"Error checking if data source exists {name}: {e}")
        return False 
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select
from utils.log import setup_logger
from datetime import datetime, timezone
from database import get_plant_db
from models.plant_models import Tag, TimeSeries, Alerts

logger = setup_logger(__name__)

async def insert_opcua_data(session: AsyncSession, workspace_id: int, tag_id: str, timestamp: datetime, value, plant_id: str, status="Good", frequency="1s"):
    """
    Insert OPC UA data into the time series table.
    
    Args:
        session (AsyncSession): Database session for the plant
        workspace_id (int): Workspace ID
        tag_id (str): Tag identifier/name
        timestamp (datetime): Data timestamp
        value: Data value
        plant_id (str): Plant ID for database context
        status (str): Data quality status
        frequency (str): Data frequency
    """
    try:
        # Convert value to string for storage if needed
        if value is None:
            # Handle null values with an empty string
            value = ""
        elif not isinstance(value, (str, int, float, bool)):
            value = str(value)
            
        # Ensure timestamp is timezone-naive for PostgreSQL
        if hasattr(timestamp, 'tzinfo') and timestamp.tzinfo is not None:
            # Convert to UTC then remove timezone info
            timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)
            
        # Check if tag exists, if not create it
        result = await session.execute(
            select(Tag).where(Tag.name == tag_id, Tag.plant_id == int(plant_id))
        )
        tag = result.scalar_one_or_none()
        
        if not tag:
            # Tag doesn't exist, create it
            tag = Tag(
                name=tag_id,
                description=f"OPC UA Tag: {tag_id}",
                unit_of_measure="None",
                plant_id=int(plant_id),
                is_active=True
            )
            session.add(tag)
            await session.flush()  # Get the tag ID
                
        # Insert time series data
        time_series = TimeSeries(
            workspace_id=workspace_id,
            tag_id=tag.id,
            timestamp=timestamp,
            value=str(value),  # Ensure value is string
            frequency=frequency,
            quality=status
        )
        
        session.add(time_series)
        await session.commit()
        
        logger.info(f"Inserted data for tag: {tag_id}, value: {value} in workspace {workspace_id}, plant {plant_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error inserting data for tag {tag_id} in workspace {workspace_id}, plant {plant_id}: {e}")
        await session.rollback()
        
        # Log error and also try to record in alerts table if possible
        error_msg = f"Error saving data: {str(e)}"
        try:
            # Try to record the error in the alerts table
            alert = Alerts(
                workspace_id=workspace_id,
                tag_id=tag.id if 'tag' in locals() and tag else None,
                timestamp=datetime.now().replace(tzinfo=None),  # Ensure timezone-naive
                message=error_msg,
                severity="error",
                is_acknowledged=False
            )
            session.add(alert)
            await session.commit()
            logger.info(f"Recorded alert for tag: {tag_id} in workspace {workspace_id}, plant {plant_id}")
        except Exception as alert_error:
            # If we can't record the alert, just log it
            logger.error(f"Failed to record alert: {alert_error}")
            logger.error(f"Alert (not saved to DB): Tag ID: {tag.id if 'tag' in locals() and tag else 'unknown'}, " 
                       f"Time: {timestamp}, Message: {error_msg}")
        
        return False

async def insert_opcua_data_batch(session: AsyncSession, workspace_id: int, data: list, plant_id: str):
    """
    Insert multiple OPC UA data points in a batch.
    
    Args:
        session (AsyncSession): Database session for the plant
        workspace_id (int): Workspace ID
        data (list): List of data dictionaries
        plant_id (str): Plant ID for database context
    """
    try:
        for item in data:
            tag_name = item["tag_name"]
            value = item["value"]
            timestamp = item["timestamp"]
            frequency = item.get("frequency", "1s")  # Default to 1s if not provided
            
            # Ensure timestamp is timezone-naive for PostgreSQL
            if hasattr(timestamp, 'tzinfo') and timestamp.tzinfo is not None:
                # Convert to UTC then remove timezone info
                timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)
            
            # Handle null values
            if value is None:
                value = ""
            
            # Check if tag exists
            result = await session.execute(
                select(Tag).where(Tag.name == tag_name, Tag.plant_id == int(plant_id))
            )
            tag = result.scalar_one_or_none()
            
            if not tag:
                # Tag doesn't exist, create it
                tag = Tag(
                    name=tag_name,
                    description=f"OPC UA Tag: {tag_name}",
                    unit_of_measure="None",
                    plant_id=int(plant_id),
                    is_active=True
                )
                session.add(tag)
                await session.flush()  # Get the tag ID

            # Insert time series data
            time_series = TimeSeries(
                workspace_id=workspace_id,
                tag_id=tag.id,
                timestamp=timestamp,
                value=str(value),  # Ensure value is string
                frequency=frequency,
                quality="Good"
            )
            
            session.add(time_series)

        await session.commit()
        logger.info(f"Inserted batch data for {len(data)} tags in workspace {workspace_id}, plant {plant_id}")
        return True
            
    except Exception as e:
        logger.error(f"Error inserting batch data in workspace {workspace_id}, plant {plant_id}: {e}")
        await session.rollback()
        
        # Try to record the error in the alerts table
        error_msg = f"Error processing batch data: {str(e)}"
        try:
            alert = Alerts(
                workspace_id=workspace_id,
                tag_id=tag.id if 'tag' in locals() and tag else None,
                timestamp=datetime.now().replace(tzinfo=None),  # Ensure timezone-naive
                message=error_msg,
                severity="error",
                is_acknowledged=False
            )
            session.add(alert)
            await session.commit()
            logger.info(f"Recorded batch error alert in workspace {workspace_id}, plant {plant_id}")
        except Exception as alert_error:
            # If we can't record the alert, just log it
            logger.error(f"Failed to record batch error alert: {alert_error}")
            logger.error(f"Alert (not saved to DB): Workspace: {workspace_id}, Plant: {plant_id}, "
                        f"Time: {datetime.now().replace(tzinfo=None)}, Message: {error_msg}")
        
        return False

async def get_tag_data(session: AsyncSession, workspace_id: int, tag_name: str, plant_id: str, start_time=None, end_time=None, limit=1000):
    """
    Retrieve time series data for a specific tag.
    
    Args:
        session (AsyncSession): Database session for the plant
        workspace_id (int): Workspace ID
        tag_name (str): Tag name
        plant_id (str): Plant ID for database context
        start_time (datetime, optional): Start time filter
        end_time (datetime, optional): End time filter
        limit (int): Maximum number of records to return
        
    Returns:
        list: List of time series data points
    """
    try:
        # Find the tag first
        result = await session.execute(
            select(Tag).where(Tag.name == tag_name, Tag.plant_id == int(plant_id))
        )
        tag = result.scalar_one_or_none()
        
        if not tag:
            logger.warning(f"Tag {tag_name} not found in plant {plant_id}")
            return []
        
        # Build query for time series data
        query = select(TimeSeries).where(
            TimeSeries.workspace_id == workspace_id,
            TimeSeries.tag_id == tag.id
        )
        
        # Add time filters if provided
        if start_time:
            query = query.where(TimeSeries.timestamp >= start_time)
        if end_time:
            query = query.where(TimeSeries.timestamp <= end_time)
        
        # Order by timestamp and limit
        query = query.order_by(TimeSeries.timestamp.desc()).limit(limit)
        
        result = await session.execute(query)
        time_series_data = result.scalars().all()
        
        logger.info(f"Retrieved {len(time_series_data)} data points for tag {tag_name} in workspace {workspace_id}, plant {plant_id}")
        return [
            {
                "timestamp": ts.timestamp,
                "value": ts.value,
                "frequency": ts.frequency,
                "quality": ts.quality
            }
            for ts in time_series_data
        ]
    except Exception as e:
        logger.error(f"Error retrieving data for tag {tag_name} in workspace {workspace_id}, plant {plant_id}: {e}")
        return []

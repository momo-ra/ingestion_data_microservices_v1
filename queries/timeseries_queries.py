"""
Time Series Queries

This module provides database queries related to time series data.
Updated for multi-database architecture with plant-specific databases.
"""

from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from models.plant_models import TimeSeries, Tag
from datetime import datetime, timedelta
from utils.log import setup_logger
from queries.tag_queries import validate_opcua_connection_string


logger = setup_logger(__name__)

async def save_plant_node_data_to_db(session: AsyncSession, node_id: str, node_data: dict, plant_id: str, frequency: str = "1m"):
    """Save node data to the plant database (plant-level, no workspace)
    
    Args:
        session: Database session for the specific plant
        node_id (str): The OPC UA connection string (e.g., "ns=3;i=1002")
        node_data (dict): The node data containing value, timestamp, and status
        plant_id (str): The plant ID for logging
        frequency (str, optional): The polling frequency. Defaults to "1m".
    
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
            logger.error(f"No tag found with connection_string {node_id} in plant {plant_id}")
            return False
        
        tag_id = tag.id
        
        # Store the original timestamp for logging
        original_timestamp = None
        if "timestamp" in node_data and node_data["timestamp"]:
            try:
                # Parse the timestamp string to datetime for logging purposes
                original_timestamp = datetime.fromisoformat(node_data["timestamp"])
                if original_timestamp.tzinfo is not None:
                    original_timestamp = original_timestamp.replace(tzinfo=None)
            except Exception as e:
                logger.warning(f"Error parsing timestamp {node_data['timestamp']} for plant {plant_id}: {e}")
                original_timestamp = None
        
        # Always use current time for database storage to ensure fresh data
        timestamp = datetime.now()
            
        # Convert value to string if it's not already
        value = str(node_data["value"])
        
        # Quality indicator from node_data if available
        quality = node_data.get("status", "GOOD")
        
        # Insert new record with current timestamp (plant-level, no workspace_id)
        time_series = TimeSeries(
            tag_id=tag_id,
            timestamp=timestamp,
            value=value,
            frequency=frequency,
            quality=str(quality)
        )
        
        session.add(time_series)
        
        try:
            await session.commit()
            if original_timestamp:
                logger.info(f"Saved plant-level data for connection_string {node_id} (tag: {tag.name}) in plant {plant_id}: value={value}, original_timestamp={original_timestamp}, stored_timestamp={timestamp}")
            else:
                logger.info(f"Saved plant-level data for connection_string {node_id} (tag: {tag.name}) in plant {plant_id}: value={value}, timestamp={timestamp}")
            return True
        except Exception as db_error:
            if "duplicate key value" in str(db_error):
                # In the rare case of timestamp collision, add a small offset
                await session.rollback()
                
                # Add a small offset (microseconds) to the timestamp
                timestamp = timestamp.replace(microsecond=timestamp.microsecond + 1)
                
                time_series = TimeSeries(
                    tag_id=tag_id,
                    timestamp=timestamp,
                    value=value,
                    frequency=frequency,
                    quality=str(quality)
                )
                
                session.add(time_series)
                await session.commit()
                logger.info(f"Saved plant-level data for connection_string {node_id} (tag: {tag.name}) in plant {plant_id} with adjusted timestamp: value={value}, timestamp={timestamp}")
                return True
            else:
                # Re-raise if it's not a duplicate key error
                raise
        
    except Exception as e:
        logger.error(f"Error saving plant-level data for connection_string {node_id} in plant {plant_id} to database: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await session.rollback()
        return False

async def save_node_data_to_db(session: AsyncSession, node_id: str, node_data: dict, plant_id: str, frequency: str = "1m"):
    """Save node data to the plant database (plant-level, no workspace)
    
    Args:
        session: Database session for the specific plant
        node_id (str): The OPC UA connection string (e.g., "ns=3;i=1002")
        node_data (dict): The node data containing value, timestamp, and status
        plant_id (str): The plant ID for logging
        frequency (str, optional): The polling frequency. Defaults to "1m".
    
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
            logger.error(f"No tag found with connection_string {node_id} in plant {plant_id}")
            return False
        
        tag_id = tag.id
        
        # Store the original timestamp for logging
        original_timestamp = None
        if "timestamp" in node_data and node_data["timestamp"]:
            try:
                # Parse the timestamp string to datetime for logging purposes
                original_timestamp = datetime.fromisoformat(node_data["timestamp"])
                if original_timestamp.tzinfo is not None:
                    original_timestamp = original_timestamp.replace(tzinfo=None)
            except Exception as e:
                logger.warning(f"Error parsing timestamp {node_data['timestamp']} for plant {plant_id}: {e}")
                original_timestamp = None
        
        # Always use current time for database storage to ensure fresh data
        timestamp = datetime.now()
            
        # Convert value to string if it's not already
        value = str(node_data["value"])
        
        # Quality indicator from node_data if available
        quality = node_data.get("status", "GOOD")
        
        # Insert new record with current timestamp (plant-level, no workspace_id)
        time_series = TimeSeries(
            tag_id=tag_id,
            timestamp=timestamp,
            value=value,
            frequency=frequency,
            quality=str(quality)
        )
        
        session.add(time_series)
        
        try:
            await session.commit()
            if original_timestamp:
                logger.info(f"Saved data for connection_string {node_id} (tag: {tag.name}) in plant {plant_id}: value={value}, original_timestamp={original_timestamp}, stored_timestamp={timestamp}")
            else:
                logger.info(f"Saved data for connection_string {node_id} (tag: {tag.name}) in plant {plant_id}: value={value}, timestamp={timestamp}")
            return True
        except Exception as db_error:
            if "duplicate key value" in str(db_error):
                # In the rare case of timestamp collision, add a small offset
                await session.rollback()
                
                # Add a small offset (microseconds) to the timestamp
                timestamp = timestamp.replace(microsecond=timestamp.microsecond + 1)
                
                time_series = TimeSeries(
                    tag_id=tag_id,
                    timestamp=timestamp,
                    value=value,
                    frequency=frequency,
                    quality=str(quality)
                )
                
                session.add(time_series)
                await session.commit()
                logger.info(f"Saved data for connection_string {node_id} (tag: {tag.name}) in plant {plant_id} with adjusted timestamp: value={value}, timestamp={timestamp}")
                return True
            else:
                # Re-raise if it's not a duplicate key error
                raise
        
    except Exception as e:
        logger.error(f"Error saving data for connection_string {node_id} in plant {plant_id} to database: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await session.rollback()
        return False

async def get_latest_node_data(session: AsyncSession, node_id: str, plant_id: str):
    """Get the latest data for a node from the plant database (plant-level, no workspace)
    
    Args:
        session: Database session for the specific plant
        node_id (str): The OPC UA connection string (e.g., "ns=3;i=1002")
        plant_id (str): The plant ID for logging
        
    Returns:
        dict: The latest data for the node or None if not found
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
            logger.warning(f"No tag found with connection_string {node_id} in plant {plant_id}")
            return None
        
        tag_id = tag.id
        
        # Get the latest record for this tag (plant-level, no workspace filtering)
        query = (
            select(TimeSeries)
            .where(TimeSeries.tag_id == tag_id)
            .order_by(TimeSeries.timestamp.desc())
            .limit(1)
        )
        
        result = await session.execute(query)
        record = result.scalars().first()
        
        if record:
            return {
                "node_id": node_id,
                "tag_id": tag_id,
                "tag_name": tag.name,
                "value": record.value,
                "timestamp": record.timestamp,
                "frequency": record.frequency,
                "quality": record.quality
            }
        else:
            logger.debug(f"No data found for connection_string {node_id} (tag: {tag.name}) in plant {plant_id}")
            return None
    except Exception as e:
        logger.error(f"Error getting latest data for connection_string {node_id} in plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

async def get_node_data_history(session: AsyncSession, node_id: str, plant_id: str, start_time: datetime = None, end_time: datetime = None, limit: int = 100):
    """Get historical data for a node from the plant database (plant-level, no workspace)
    
    Args:
        session: Database session for the specific plant
        node_id (str): The OPC UA connection string (e.g., "ns=3;i=1002")
        plant_id (str): The plant ID for logging
        start_time (datetime, optional): The start time. Defaults to None.
        end_time (datetime, optional): The end time. Defaults to None.
        limit (int, optional): The maximum number of records to return. Defaults to 100.
        
    Returns:
        list: The historical data for the node
    """
    try:
        # Validate OPC UA connection string format
        if not validate_opcua_connection_string(node_id):
            logger.error(f"Invalid OPC UA connection string format: {node_id}. Expected format: ns=<namespace>;<identifier_type>=<identifier>")
            return []
            
        # Find tag by connection_string instead of name
        result = await session.execute(
            select(Tag).where(Tag.connection_string == node_id)
        )
        tag = result.scalars().first()
        
        if not tag:
            logger.warning(f"No tag found with connection_string {node_id} in plant {plant_id}")
            return []
        
        tag_id = tag.id
        
        # Set default times if not provided
        if end_time is None:
            end_time = datetime.now()
        elif end_time.tzinfo is not None:
            # Remove timezone info if present
            end_time = end_time.replace(tzinfo=None)
            
        if start_time is None:
            start_time = end_time - timedelta(days=1)
        elif start_time.tzinfo is not None:
            # Remove timezone info if present
            start_time = start_time.replace(tzinfo=None)
            
        # Build query (plant-level, no workspace filtering)
        query = select(TimeSeries).where(TimeSeries.tag_id == tag_id)
        
        # Add time filters
        query = query.where(TimeSeries.timestamp >= start_time)
        query = query.where(TimeSeries.timestamp <= end_time)
        
        # Order by timestamp and limit results
        query = query.order_by(TimeSeries.timestamp.desc()).limit(limit)
        
        # Execute query
        result = await session.execute(query)
        records = result.scalars().all()
        
        # Convert to list of dictionaries
        history = []
        for record in records:
            history.append({
                "node_id": node_id,
                "tag_id": tag_id,
                "tag_name": tag.name,
                "value": record.value,
                "timestamp": record.timestamp,
                "frequency": record.frequency,
                "quality": record.quality
            })
        
        logger.debug(f"Retrieved {len(history)} historical records for connection_string {node_id} (tag: {tag.name}) in plant {plant_id}")
        return history
        
    except Exception as e:
        logger.error(f"Error getting historical data for connection_string {node_id} in plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

async def get_node_data_statistics(session: AsyncSession, node_id: str, plant_id: str, start_time: datetime = None, end_time: datetime = None):
    """Get statistics for node data from the plant database (plant-level, no workspace)
    
    Args:
        session: Database session for the specific plant
        node_id (str): The OPC UA connection string (e.g., "ns=3;i=1002")
        plant_id (str): The plant ID for logging
        start_time (datetime, optional): The start time. Defaults to None.
        end_time (datetime, optional): The end time. Defaults to None.
        
    Returns:
        dict: Statistics for the node data
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
            logger.warning(f"No tag found with connection_string {node_id} in plant {plant_id}")
            return None
        
        tag_id = tag.id
        
        # Set default times if not provided
        if end_time is None:
            end_time = datetime.now()
        elif end_time.tzinfo is not None:
            end_time = end_time.replace(tzinfo=None)
            
        if start_time is None:
            start_time = end_time - timedelta(days=1)
        elif start_time.tzinfo is not None:
            start_time = start_time.replace(tzinfo=None)
            
        # Build base query (plant-level, no workspace filtering)
        base_query = select(TimeSeries).where(
            (TimeSeries.tag_id == tag_id) &
            (TimeSeries.timestamp >= start_time) &
            (TimeSeries.timestamp <= end_time)
        )
        
        # Convert values to numeric for calculations (skip non-numeric values)
        numeric_query = base_query.where(text("value ~ '^[0-9]+\\.?[0-9]*$'"))
        
        # Execute query for numeric statistics
        result = await session.execute(numeric_query)
        numeric_records = result.scalars().all()
        
        if not numeric_records:
            return {
                "node_id": node_id,
                "tag_id": tag_id,
                "tag_name": tag.name,
                "start_time": start_time,
                "end_time": end_time,
                "total_records": 0,
                "numeric_records": 0,
                "statistics": None
            }
        
        # Calculate statistics
        values = [float(record.value) for record in numeric_records]
        
        statistics = {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "sum": sum(values)
        }
        
        # Get total record count (including non-numeric)
        total_result = await session.execute(
            select(func.count(TimeSeries.id)).where(
                (TimeSeries.tag_id == tag_id) &
                (TimeSeries.timestamp >= start_time) &
                (TimeSeries.timestamp <= end_time)
            )
        )
        total_count = total_result.scalar()
        
        return {
            "node_id": node_id,
            "tag_id": tag_id,
            "tag_name": tag.name,
            "start_time": start_time,
            "end_time": end_time,
            "total_records": total_count,
            "numeric_records": len(values),
            "statistics": statistics
        }
        
    except Exception as e:
        logger.error(f"Error getting statistics for connection_string {node_id} in plant {plant_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

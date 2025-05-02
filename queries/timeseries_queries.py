"""
Time Series Queries

This module provides database queries related to time series data.
"""

from database import async_session
from sqlalchemy import select, func, text
from models.models import Tag, TimeSeries
from datetime import datetime
from utils.log import setup_logger
from queries.polling_queries import get_or_create_tag_id

logger = setup_logger(__name__)

async def save_node_data_to_db(node_id, node_data, frequency="1m"):
    """Save node data to the database
    
    Args:
        node_id (str): The node ID
        node_data (dict): The node data containing value, timestamp, and status
        frequency (str, optional): The polling frequency. Defaults to "1m".
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # استخدام node_id مباشرة كاسم للعلامة بدون تجزئة
        tag_name = node_id
            
        # Get tag_id from tag name
        tag_id = await get_or_create_tag_id(tag_name)
        
        # Store the original timestamp for logging
        original_timestamp = None
        if "timestamp" in node_data and node_data["timestamp"]:
            try:
                # Parse the timestamp string to datetime for logging purposes
                original_timestamp = datetime.fromisoformat(node_data["timestamp"])
                if original_timestamp.tzinfo is not None:
                    original_timestamp = original_timestamp.replace(tzinfo=None)
            except Exception as e:
                logger.warning(f"Error parsing timestamp {node_data['timestamp']}: {e}")
                original_timestamp = None
        
        # Always use current time for database storage to ensure fresh data
        timestamp = datetime.now()
            
        # Convert value to string if it's not already
        value = str(node_data["value"])
        
        # Insert new record with current timestamp
        async with async_session() as session:
            time_series = TimeSeries(
                tag_id=tag_id,
                timestamp=timestamp,
                value=value,
                frequency=frequency
            )
            
            session.add(time_series)
            
            try:
                await session.commit()
                if original_timestamp:
                    logger.info(f"Saved data for node {node_id}: value={value}, original_timestamp={original_timestamp}, stored_timestamp={timestamp}")
                else:
                    logger.info(f"Saved data for node {node_id}: value={value}, timestamp={timestamp}")
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
                        frequency=frequency
                    )
                    
                    session.add(time_series)
                    await session.commit()
                    logger.info(f"Saved data for node {node_id} with adjusted timestamp: value={value}, timestamp={timestamp}")
                    return True
                else:
                    # Re-raise if it's not a duplicate key error
                    raise
        
    except Exception as e:
        logger.error(f"Error saving data for node {node_id} to database: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def get_latest_node_data(node_id):
    """Get the latest data for a node
    
    Args:
        node_id (str): The node ID
        
    Returns:
        dict: The latest data for the node
    """
    try:
        # استخدام node_id مباشرة كاسم للعلامة بدون تجزئة
        tag_name = node_id
            
        # Get tag_id
        tag_id = await get_or_create_tag_id(tag_name)
        
        async with async_session() as session:
            # Get the latest record for this tag
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
                    "value": record.value,
                    "timestamp": record.timestamp,
                    "frequency": record.frequency
                }
            else:
                logger.warning(f"No data found for node {node_id}")
                return None
    except Exception as e:
        logger.error(f"Error getting latest data for node {node_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

async def get_node_data_history(node_id, start_time=None, end_time=None, limit=100):
    """Get historical data for a node
    
    Args:
        node_id (str): The node ID
        start_time (datetime, optional): The start time. Defaults to None.
        end_time (datetime, optional): The end time. Defaults to None.
        limit (int, optional): The maximum number of records to return. Defaults to 100.
        
    Returns:
        list: The historical data for the node
    """
    try:
        # استخدام node_id مباشرة كاسم للعلامة بدون تجزئة
        tag_name = node_id
            
        # Get tag_id
        tag_id = await get_or_create_tag_id(tag_name)
        
        # Set default times if not provided
        if end_time is None:
            end_time = datetime.now()
        elif end_time.tzinfo is not None:
            # Remove timezone info if present
            end_time = end_time.replace(tzinfo=None)
            
        if start_time is None:
            start_time = end_time - datetime.timedelta(days=1)
        elif start_time.tzinfo is not None:
            # Remove timezone info if present
            start_time = start_time.replace(tzinfo=None)
            
        async with async_session() as session:
            # Build query
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
                    "value": record.value,
                    "timestamp": record.timestamp,
                    "frequency": record.frequency
                })
            
            return history
    except Exception as e:
        logger.error(f"Error getting history for node {node_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

async def get_node_data_statistics(node_id, start_time=None, end_time=None):
    """Get statistics for a node's data
    
    Args:
        node_id (str): The node ID
        start_time (datetime, optional): The start time. Defaults to None.
        end_time (datetime, optional): The end time. Defaults to None.
        
    Returns:
        dict: The statistics for the node's data
    """
    try:
        # استخدام node_id مباشرة كاسم للعلامة بدون تجزئة
        tag_name = node_id
            
        # Get tag_id
        tag_id = await get_or_create_tag_id(tag_name)
        
        # Set default times if not provided
        if end_time is None:
            end_time = datetime.now()
        elif end_time.tzinfo is not None:
            # Remove timezone info if present
            end_time = end_time.replace(tzinfo=None)
            
        if start_time is None:
            start_time = end_time - datetime.timedelta(days=1)
        elif start_time.tzinfo is not None:
            # Remove timezone info if present
            start_time = start_time.replace(tzinfo=None)
            
        async with async_session() as session:
            # Build query for count
            count_query = (
                select(func.count())
                .select_from(TimeSeries)
                .where(TimeSeries.tag_id == tag_id)
                .where(TimeSeries.timestamp >= start_time)
                .where(TimeSeries.timestamp <= end_time)
            )
            
            # Execute count query
            count_result = await session.execute(count_query)
            count = count_result.scalar()
            
            # If no data, return empty statistics
            if count == 0:
                return {
                    "node_id": node_id,
                    "tag_id": tag_id,
                    "count": 0,
                    "start_time": start_time,
                    "end_time": end_time,
                    "min_value": None,
                    "max_value": None,
                    "avg_value": None
                }
            
            # Get min, max, avg values
            # Note: This assumes values can be converted to numeric
            # You might need to adjust this based on your data types
            stats_query = (
                select(
                    func.min(TimeSeries.value.cast(float)),
                    func.max(TimeSeries.value.cast(float)),
                    func.avg(TimeSeries.value.cast(float))
                )
                .select_from(TimeSeries)
                .where(TimeSeries.tag_id == tag_id)
                .where(TimeSeries.timestamp >= start_time)
                .where(TimeSeries.timestamp <= end_time)
            )
            
            # Execute stats query
            stats_result = await session.execute(stats_query)
            min_value, max_value, avg_value = stats_result.first()
            
            return {
                "node_id": node_id,
                "tag_id": tag_id,
                "count": count,
                "start_time": start_time,
                "end_time": end_time,
                "min_value": min_value,
                "max_value": max_value,
                "avg_value": avg_value
            }
    except Exception as e:
        logger.error(f"Error getting statistics for node {node_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            "node_id": node_id,
            "error": str(e),
            "count": 0
        } 
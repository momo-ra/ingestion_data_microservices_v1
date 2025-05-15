from database import async_session
from sqlalchemy import text
from schemas.schema import TagSchema, TimeSeriesSchema, AlertSchema
from utils.log import setup_logger
from datetime import datetime, timezone
from models.models import Tag, TimeSeries, Alerts

logger = setup_logger(__name__)

async def insert_opcua_data(tag_id, timestamp, value, status="Good", frequency=1):
    async with async_session() as session:
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
                
            # Check if tag exists, if not create it using raw SQL
            tag_query = text(f"SELECT id FROM {Tag.__tablename__} WHERE name = :name")
            tag_result = await session.execute(tag_query, {"name": tag_id})
            tag_row = tag_result.fetchone()
            
            if tag_row is None:
                # Tag doesn't exist, create it
                tag_insert = text(f"""
                    INSERT INTO {Tag.__tablename__} (name, description, unit_of_measure) 
                    VALUES (:name, :description, :unit_of_measure)
                    RETURNING id
                """)
                tag_result = await session.execute(
                    tag_insert, 
                    {
                        "name": tag_id,
                        "description": f"OPC UA Tag: {tag_id}",
                        "unit_of_measure": "None"
                    }
                )
                tag_id_db = tag_result.scalar_one()
            else:
                tag_id_db = tag_row[0]
                
            # Insert time series data - add required 'frequency' field
            time_series_insert = text(f"""
                INSERT INTO {TimeSeries.__tablename__} (tag_id, timestamp, value, frequency)
                VALUES (:tag_id, :timestamp, :value, :frequency)
            """)
            await session.execute(
                time_series_insert,
                {
                    "tag_id": tag_id_db,
                    "timestamp": timestamp,
                    "value": str(value),  # Ensure value is string
                    "frequency": str(frequency)  # Convert frequency to string
                }
            )
            
            # Commit the transaction
            await session.commit()
            logger.info(f"Inserted data for tag: {tag_id}, value: {value}")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting data for tag {tag_id}: {e}")
            await session.rollback()
            
            # Log error and also try to record in alerts table if possible
            error_msg = f"Error saving data: {str(e)}"
            try:
                # Try to record the error in the alerts table
                alert_insert = text(f"""
                    INSERT INTO {Alerts.__tablename__} (tag_id, timestamp, message)
                    VALUES (:tag_id, :timestamp, :message)
                """)
                await session.execute(
                    alert_insert,
                    {
                        "tag_id": tag_id_db if 'tag_id_db' in locals() else None,
                        "timestamp": datetime.now().replace(tzinfo=None),  # Ensure timezone-naive
                        "message": error_msg
                    }
                )
                await session.commit()
                logger.info(f"Recorded alert for tag: {tag_id}")
            except Exception as alert_error:
                # If we can't record the alert, just log it
                logger.error(f"Failed to record alert: {alert_error}")
                logger.error(f"Alert (not saved to DB): Tag ID: {tag_id_db if 'tag_id_db' in locals() else 'unknown'}, " 
                           f"Time: {timestamp}, Message: {error_msg}")
            
            return False

# Keep the original function for backwards compatibility but mark as deprecated
async def insert_opcua_data_batch(data):

    async with async_session() as session:
        try:
            for item in data:
                tag_name = item["tag_name"]
                value = item["value"]
                timestamp = item["timestamp"]
                frequency = item.get("frequency", 1)  # Default to 1 if not provided
                
                # Ensure timestamp is timezone-naive for PostgreSQL
                if hasattr(timestamp, 'tzinfo') and timestamp.tzinfo is not None:
                    # Convert to UTC then remove timezone info
                    timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)
                
                # Handle null values
                if value is None:
                    value = ""
                
                # Check if tag exists
                tag_query = text(f"SELECT id FROM {Tag.__tablename__} WHERE name = :name")
                tag_result = await session.execute(tag_query, {"name": tag_name})
                tag_row = tag_result.fetchone()
                
                if tag_row is None:
                    # Tag doesn't exist, create it
                    tag_insert = text(f"""
                        INSERT INTO {Tag.__tablename__} (name, description, unit_of_measure) 
                        VALUES (:name, :description, :unit_of_measure)
                        RETURNING id
                    """)
                    tag_result = await session.execute(
                        tag_insert, 
                        {
                            "name": tag_name,
                            "description": f"OPC UA Tag: {tag_name}",
                            "unit_of_measure": "None"
                        }
                    )
                    tag_id = tag_result.scalar_one()
                else:
                    tag_id = tag_row[0]

                # Insert time series data - added frequency field
                time_series_query = text(f"""
                    INSERT INTO {TimeSeries.__tablename__} (tag_id, timestamp, value, frequency)
                    VALUES (:tag_id, :timestamp, :value, :frequency)
                """)
                await session.execute(
                    time_series_query,
                    {
                        "tag_id": tag_id,
                        "timestamp": timestamp,
                        "value": str(value),  # Ensure value is string
                        "frequency": str(frequency)  # Convert frequency to string
                    }
                )

                await session.commit()
                logger.info(f"Inserted data for tag: {tag_name}")
                
        except Exception as e:
            logger.error(f"Error inserting data for tag: {tag_name}")
            logger.error(str(e))
            await session.rollback()
            
            # Try to record the error in the alerts table
            error_msg = f"Error processing data: {str(e)}"
            try:
                alert_query = text(f"""
                    INSERT INTO {Alerts.__tablename__} (tag_id, timestamp, message)
                    VALUES (:tag_id, :timestamp, :message)
                """)
                await session.execute(
                    alert_query,
                    {
                        "tag_id": tag_id if 'tag_id' in locals() else None,
                        "timestamp": datetime.now().replace(tzinfo=None),  # Ensure timezone-naive
                        "message": error_msg
                    }
                )
                await session.commit()
                logger.info(f"Recorded alert for tag: {tag_name}")
            except Exception as alert_error:
                # If we can't record the alert, just log it
                logger.error(f"Failed to record alert: {alert_error}")
                logger.error(f"Alert (not saved to DB): Tag ID: {tag_id if 'tag_id' in locals() else 'unknown'}, " 
                            f"Time: {timestamp if 'timestamp' in locals() else datetime.now().replace(tzinfo=None)}, "
                            f"Message: {error_msg}")
            
            return False
        return True 
    
# async def save_polling_task(tag_id, time_interval):
#     async with async_session() as session:
#         try:

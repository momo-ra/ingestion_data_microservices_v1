from database import async_session
from sqlalchemy import insert
from schemas.schema import AlertSchema, TagSchema
from datetime import datetime
from utils.log import setup_logger

logger = setup_logger(__name__)

async def insert_alert(alert_data):
    """
    Insert an alert into the alert table.
    
    Args:
        alert_data (dict): Dictionary containing alert data with keys:
            - tag_name (str): Name of the tag
            - message (str): Alert message
            - timestamp (datetime): Timestamp of the alert
    """
    async with async_session() as session:
        try:
            # First find or create the tag
            tag_name = alert_data.get("tag_name", "unknown")
            
            # Create tag entry if it doesn't exist
            tag_query = insert(TagSchema).values(
                name=tag_name,
                description=f"OPC UA Tag: {tag_name}",
                unit_of_measure="None"
            )
            tag_result = await session.execute(tag_query)
            tag_id = tag_result.scalar_one().id
            
            # Create alert entry
            alert_query = insert(AlertSchema).values(
                tag_id=tag_id,
                timestamp=alert_data.get("timestamp", datetime.now()),
                message=alert_data.get("message", "Unknown error")
            )
            
            await session.execute(alert_query)
            await session.commit()
            
            logger.success(f"Alert logged for tag: {tag_name}")
            return True
        except Exception as e:
            logger.danger(f"Error logging alert: {e}")
            await session.rollback()
            return False

async def get_alerts(tag_id=None, start_time=None, end_time=None, limit=100):
    """
    Retrieve alerts from the database with optional filtering.
    
    Args:
        tag_id (int, optional): Filter by tag ID
        start_time (datetime, optional): Filter alerts after this time
        end_time (datetime, optional): Filter alerts before this time
        limit (int, optional): Limit the number of results
        
    Returns:
        list: List of alert records
    """
    # This implementation can be expanded as needed
    pass

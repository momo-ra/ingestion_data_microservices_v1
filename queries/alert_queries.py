from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert, select, text
from schemas.schema import AlertSchema, TagSchema
from datetime import datetime
from utils.log import setup_logger
from database import get_plant_db, get_central_db
from models.plant_models import Tag, Alerts

logger = setup_logger(__name__)

async def insert_alert(session: AsyncSession, workspace_id: int, alert_data: dict, plant_id: str):
    """
    Insert an alert into the alert table.
    
    Args:
        session (AsyncSession): Database session for the plant
        workspace_id (int): Workspace ID
        alert_data (dict): Dictionary containing alert data with keys:
            - tag_name (str): Name of the tag
            - message (str): Alert message
            - timestamp (datetime): Timestamp of the alert
            - severity (str, optional): Alert severity level
        plant_id (str): Plant ID for database context
    """
    try:
        # First find or create the tag
        tag_name = alert_data.get("tag_name", "unknown")
        
        # Check if tag exists
        result = await session.execute(
            select(Tag).where(Tag.name == tag_name, Tag.plant_id == int(plant_id))
        )
        tag = result.scalar_one_or_none()
        
        if not tag:
            # Create tag entry if it doesn't exist
            tag = Tag(
                name=tag_name,
                description=f"OPC UA Tag: {tag_name}",
                unit_of_measure="None",
                plant_id=int(plant_id),
                is_active=True
            )
            session.add(tag)
            await session.flush()  # Get the tag ID
        
        # Create alert entry
        alert = Alerts(
            workspace_id=workspace_id,
            tag_id=tag.id,
            timestamp=alert_data.get("timestamp", datetime.now()),
            message=alert_data.get("message", "Unknown error"),
            severity=alert_data.get("severity", "warning"),
            is_acknowledged=False
        )
        
        session.add(alert)
        await session.commit()
        
        logger.success(f"Alert logged for tag: {tag_name} in workspace {workspace_id}, plant {plant_id}")
        return True
    except Exception as e:
        logger.error(f"Error logging alert for tag {alert_data.get('tag_name', 'unknown')}: {e}")
        await session.rollback()
        return False

async def get_alerts(session: AsyncSession, workspace_id: int, plant_id: str, tag_id=None, start_time=None, end_time=None, limit=100):
    """
    Retrieve alerts from the database with optional filtering.
    
    Args:
        session (AsyncSession): Database session for the plant
        workspace_id (int): Workspace ID
        plant_id (str): Plant ID for database context
        tag_id (int, optional): Filter by tag ID
        start_time (datetime, optional): Filter alerts after this time
        end_time (datetime, optional): Filter alerts before this time
        limit (int, optional): Limit the number of results
        
    Returns:
        list: List of alert records
    """
    try:
        # Build query with workspace filter
        query = select(Alerts).where(Alerts.workspace_id == workspace_id)
        
        # Add optional filters
        if tag_id:
            query = query.where(Alerts.tag_id == tag_id)
        if start_time:
            query = query.where(Alerts.timestamp >= start_time)
        if end_time:
            query = query.where(Alerts.timestamp <= end_time)
        
        # Order by timestamp descending and limit
        query = query.order_by(Alerts.timestamp.desc()).limit(limit)
        
        result = await session.execute(query)
        alerts = result.scalars().all()
        
        logger.info(f"Retrieved {len(alerts)} alerts for workspace {workspace_id}, plant {plant_id}")
        return [alert.to_dict() for alert in alerts]
    except Exception as e:
        logger.error(f"Error retrieving alerts for workspace {workspace_id}, plant {plant_id}: {e}")
        return []

async def acknowledge_alert(session: AsyncSession, workspace_id: int, alert_id: int, acknowledged_by: int, plant_id: str):
    """
    Acknowledge an alert.
    
    Args:
        session (AsyncSession): Database session for the plant
        workspace_id (int): Workspace ID
        alert_id (int): Alert ID to acknowledge
        acknowledged_by (int): User ID who acknowledged the alert
        plant_id (str): Plant ID for database context
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Find the alert
        result = await session.execute(
            select(Alerts).where(
                Alerts.id == alert_id,
                Alerts.workspace_id == workspace_id
            )
        )
        alert = result.scalar_one_or_none()
        
        if not alert:
            logger.warning(f"Alert {alert_id} not found in workspace {workspace_id}, plant {plant_id}")
            return False
        
        # Update acknowledgment
        alert.is_acknowledged = True
        alert.acknowledged_by = acknowledged_by
        alert.acknowledged_at = datetime.now()
        
        await session.commit()
        
        logger.info(f"Alert {alert_id} acknowledged by user {acknowledged_by} in workspace {workspace_id}, plant {plant_id}")
        return True
    except Exception as e:
        logger.error(f"Error acknowledging alert {alert_id} in workspace {workspace_id}, plant {plant_id}: {e}")
        await session.rollback()
        return False

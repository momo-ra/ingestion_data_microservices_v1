from fastapi import Header, Depends
from typing import Optional
from utils.log import setup_logger

logger = setup_logger(__name__)

# Default values for backward compatibility
DEFAULT_PLANT_ID = "1"
DEFAULT_WORKSPACE_ID = 1

async def get_plant_context(
    plant_id: Optional[str] = Header(None, alias="plant-id")
) -> dict:
    """
    Get plant context for plant-level operations (no workspace required)
    
    Args:
        plant_id: Plant identifier from header (optional)
        
    Returns:
        Dictionary containing plant_id with default applied
    """
    return {
        "plant_id": plant_id or DEFAULT_PLANT_ID
    }

async def get_context_with_defaults(
    plant_id: Optional[str] = Header(None, alias="plant-id"),
    workspace_id: Optional[int] = Header(None, alias="workspace-id")
) -> dict:
    """
    Get plant and workspace context with default values for backward compatibility
    
    Args:
        plant_id: Plant identifier from header (optional)
        workspace_id: Workspace identifier from header (optional)
        
    Returns:
        Dictionary containing plant_id and workspace_id with defaults applied
    """
    return {
        "plant_id": plant_id or DEFAULT_PLANT_ID,
        "workspace_id": workspace_id or DEFAULT_WORKSPACE_ID
    } 
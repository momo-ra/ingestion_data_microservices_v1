from fastapi import APIRouter, HTTPException, Depends
from services.opc_ua_services import get_opc_ua_client
from services.subscription_services import get_subscription_service
from queries.subscription_queries import get_active_subscription_tasks
from schemas.schema import NodeRequest
from database import get_plant_db
from utils.log import setup_logger
from utils.response import success_response, fail_response
from routers.common_routers import get_context_with_defaults, DEFAULT_PLANT_ID, DEFAULT_WORKSPACE_ID

router = APIRouter(prefix="/node", tags=["nodes"])
logger = setup_logger(__name__)

@router.post("/value")
async def get_node_value(request: NodeRequest):
    """Get the current value of a specific OPC-UA node"""
    try:
        client = get_opc_ua_client()
        node_data = await client.get_value_of_specific_node(request.node_id)
        
        if node_data:
            return success_response(
                data=node_data,
                message=f"Retrieved value for node {request.node_id}"
            )
        else:
            return fail_response(
                message=f"Failed to get value for node {request.node_id}",
                data={"node_id": request.node_id}
            )
    except Exception as e:
        logger.error(f"Error getting node value: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/subscribe")
async def subscribe_to_node(
    request: NodeRequest,
    context: dict = Depends(get_context_with_defaults)
):
    """Subscribe to data changes for a specific OPC-UA node"""
    try:
        subscription_service = get_subscription_service()
        
        # Note: The subscription service currently uses internal default values for plant_id and workspace_id
        # This will be improved in future versions to use the context parameters
        handle = await subscription_service.create_subscription(request.node_id)
        
        if handle:
            return success_response(
                data={
                    "handle": str(handle),
                    "node_id": request.node_id,
                    "workspace_id": DEFAULT_WORKSPACE_ID,
                    "plant_id": DEFAULT_PLANT_ID
                },
                message=f"Subscribed to node {request.node_id} (using default workspace {DEFAULT_WORKSPACE_ID}, plant {DEFAULT_PLANT_ID})"
            )
        else:
            return fail_response(
                message=f"Failed to subscribe to node {request.node_id}",
                data={"node_id": request.node_id}
            )
    except Exception as e:
        logger.error(f"Error subscribing to node: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/unsubscribe")
async def unsubscribe_from_node(
    request: NodeRequest,
    context: dict = Depends(get_context_with_defaults)
):
    """Unsubscribe from data changes for a specific OPC-UA node"""
    try:
        subscription_service = get_subscription_service()
        
        # Note: The subscription service currently uses internal default values for plant_id and workspace_id
        # This will be improved in future versions to use the context parameters
        success = await subscription_service.remove_subscription(request.node_id)
        
        if success:
            return success_response(
                data={
                    "node_id": request.node_id,
                    "workspace_id": DEFAULT_WORKSPACE_ID,
                    "plant_id": DEFAULT_PLANT_ID
                },
                message=f"Unsubscribed from node {request.node_id} (from default workspace {DEFAULT_WORKSPACE_ID}, plant {DEFAULT_PLANT_ID})"
            )
        else:
            return fail_response(
                message=f"Failed to unsubscribe from node {request.node_id}",
                data={"node_id": request.node_id}
            )
    except Exception as e:
        logger.error(f"Error unsubscribing from node: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/subscribe/active")
async def get_active_subscriptions(context: dict = Depends(get_context_with_defaults)):
    """Get all active subscriptions"""
    try:
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            tasks = await get_active_subscription_tasks(
                session,
                context["workspace_id"],
                context["plant_id"]
            )
            return success_response(
                data={
                    "active_subscriptions": tasks,
                    "workspace_id": context["workspace_id"],
                    "plant_id": context["plant_id"],
                    "count": len(tasks)
                },
                message=f"Retrieved {len(tasks)} active subscriptions"
            )
            break
    except Exception as e:
        logger.error(f"Error getting active subscriptions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/subscribe/count")
async def get_subscription_count(context: dict = Depends(get_context_with_defaults)):
    """Get the count of active subscriptions in memory and database"""
    try:
        # Get count from service instance
        subscription_service = get_subscription_service()
        memory_count = len(subscription_service.subscription_handles)
        
        # Get count from database
        async for session in get_plant_db(context["plant_id"]):
            db_tasks = await get_active_subscription_tasks(
                session,
                context["workspace_id"],
                context["plant_id"]
            )
            db_count = len(db_tasks)
            break
        
        return success_response(
            data={
                "memory_count": memory_count,
                "database_count": db_count,
                "subscription_handles": list(subscription_service.subscription_handles.keys()),
                "workspace_id": context["workspace_id"],
                "plant_id": context["plant_id"]
            },
            message=f"Retrieved subscription counts - Memory: {memory_count}, Database: {db_count}"
        )
    except Exception as e:
        logger.error(f"Error getting subscription count: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 
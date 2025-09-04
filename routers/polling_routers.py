from fastapi import APIRouter, HTTPException, Depends
from services.polling_services import get_polling_service
from queries.polling_queries import get_active_polling_tasks
from schemas.schema import NodeRequest, PollingRequest
from database import get_plant_db
from utils.log import setup_logger
from utils.response import success_response, fail_response
from routers.common_routers import get_plant_context, DEFAULT_PLANT_ID
from services.opc_ua_services import get_opc_ua_client

router = APIRouter(prefix="/node/poll", tags=["polling"])
logger = setup_logger(__name__)

@router.post("/start")
async def start_polling_node(
    request: PollingRequest,
    context: dict = Depends(get_plant_context)
):
    """Start polling a specific OPC-UA node at regular intervals"""
    try:
        polling_service = get_polling_service()
        
        # Pass the plant_id from context to the polling service
        success = await polling_service.add_polling_node(
            request.node_id, 
            request.interval_seconds, 
            context["plant_id"]
        )
        
        if success:
            return success_response(
                data={
                    "node_id": request.node_id,
                    "interval_seconds": request.interval_seconds,
                    "plant_id": context["plant_id"]
                },
                message=f"Started polling node {request.node_id} every {request.interval_seconds} seconds (plant-level, plant {context['plant_id']})"
            )
        else:
            # Check if the failure was due to node not existing
            opc_client = get_opc_ua_client()
            
            if opc_client.connected:
                try:
                    # Try to verify if the node exists
                    node = opc_client.client.get_node(request.node_id)
                    await node.read_browse_name()
                    # If we get here, the node exists, so the failure was for another reason
                    raise HTTPException(status_code=500, detail=f"Failed to start polling node {request.node_id}. The node exists but polling could not be started.")
                except Exception as e:
                    # Check if this is a specific OPC UA error indicating the node doesn't exist
                    error_str = str(e).lower()
                    if any(keyword in error_str for keyword in ['not found', 'bad node id', 'bad nodeid', 'node does not exist', 'invalid node']):
                        # Node doesn't exist in OPC UA server
                        raise HTTPException(status_code=404, detail=f"Node {request.node_id} does not exist in the OPC UA server. Cannot create polling task for non-existent node.")
                    else:
                        # Some other error occurred during node verification
                        logger.warning(f"Error verifying node {request.node_id}: {e}")
                        # Don't assume the node doesn't exist, just report the original failure
                        raise HTTPException(status_code=500, detail=f"Failed to start polling node {request.node_id}. Error: {str(e)}")
            else:
                raise HTTPException(status_code=500, detail=f"Failed to start polling node {request.node_id}. OPC UA client is not connected.")
    except HTTPException:
        # Re-raise HTTPExceptions without wrapping them
        raise
    except Exception as e:
        logger.error(f"Error starting polling: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stop")
async def stop_polling_node(
    request: NodeRequest,
    context: dict = Depends(get_plant_context)
):
    """Stop polling a specific OPC-UA node"""
    try:
        polling_service = get_polling_service()
        
        # Log current polling tasks for debugging
        logger.info(f"Attempting to stop polling for node: {request.node_id}")
        logger.info(f"Current polling tasks in memory: {list(polling_service.polling_tasks.keys())}")
        
        # Pass the plant_id from context to the polling service
        success = await polling_service.remove_polling_node(
            request.node_id, 
            context["plant_id"]
        )
        
        if success:
            return success_response(
                data={
                    "node_id": request.node_id,
                    "plant_id": context["plant_id"]
                },
                message=f"Stopped polling node {request.node_id} (plant {context['plant_id']})"
            )
        else:
            # Return 404 as a proper response, not an exception
            return fail_response(
                message=f"Polling task for node {request.node_id} not found in memory. Current tasks: {list(polling_service.polling_tasks.keys())}",
                data={
                    "node_id": request.node_id,
                    "plant_id": context["plant_id"],
                    "available_tasks": list(polling_service.polling_tasks.keys())
                }
            )
    except HTTPException:
        # Re-raise HTTPExceptions without wrapping them
        raise
    except Exception as e:
        logger.error(f"Error stopping polling: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/active")
async def get_active_polling_nodes(context: dict = Depends(get_plant_context)):
    """Get all active polling nodes"""
    try:
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            tasks = await get_active_polling_tasks(
                session, 
                context["plant_id"]
            )
            return success_response(
                data={
                    "active_polling_tasks": tasks,
                    "plant_id": context["plant_id"],
                    "count": len(tasks)
                },
                message=f"Retrieved {len(tasks)} active polling tasks for plant {context['plant_id']}"
            )
            break
    except Exception as e:
        logger.error(f"Error getting active polling nodes: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/debug")
async def get_polling_debug_info(context: dict = Depends(get_plant_context)):
    """Get detailed polling debug information comparing database vs memory"""
    try:
        polling_service = get_polling_service()
        
        # Get tasks from memory
        memory_tasks = dict(polling_service.polling_tasks)
        
        # Get tasks from database
        async for session in get_plant_db(context["plant_id"]):
            db_tasks = await get_active_polling_tasks(
                session, 
                context["plant_id"]
            )
            break
        
        return success_response(
            data={
                "memory_tasks": {
                    "count": len(memory_tasks),
                    "tasks": memory_tasks
                },
                "database_tasks": {
                    "count": len(db_tasks),
                    "tasks": db_tasks
                },
                "context": {
                    "plant_id": context["plant_id"]
                },
                "comparison": {
                    "memory_only": [key for key in memory_tasks.keys() if key not in [task['tag_name'] for task in db_tasks]],
                    "database_only": [task['tag_name'] for task in db_tasks if task['tag_name'] not in memory_tasks.keys()]
                }
            },
            message=f"Retrieved polling debug information for plant {context['plant_id']}"
        )
    except Exception as e:
        logger.error(f"Error getting polling debug info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/check-node/{node_id}")
async def check_node_exists(
    node_id: str,
    context: dict = Depends(get_plant_context)
):
    """Check if a node exists in the OPC UA server"""
    try:
        opc_client = get_opc_ua_client()
        
        if not opc_client.connected:
            return fail_response(
                message="Cannot verify node existence - OPC UA client is not connected",
                data={
                    "node_id": node_id,
                    "exists": False,
                    "error": "OPC UA client is not connected"
                }
            )
        
        try:
            # Try to get the node from the server
            node = opc_client.client.get_node(node_id)
            # Try to read a property to verify the node exists
            await node.read_browse_name()
            
            return success_response(
                data={
                    "node_id": node_id,
                    "exists": True,
                    "error": None
                },
                message=f"Node {node_id} exists in OPC UA server"
            )
        except Exception as e:
            return fail_response(
                message=f"Node {node_id} does not exist in OPC UA server",
                data={
                    "node_id": node_id,
                    "exists": False,
                    "error": str(e)
                }
            )
    except Exception as e:
        logger.error(f"Error checking node existence: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/browse-nodes")
async def browse_available_nodes(
    context: dict = Depends(get_plant_context),
    max_nodes: int = 50
):
    """Browse available nodes in the OPC UA server to see what's available"""
    try:
        opc_client = get_opc_ua_client()
        
        if not opc_client.connected:
            return {
                "error": "OPC UA client is not connected",
                "message": "Cannot browse nodes - OPC UA client is not connected",
                "server_url": opc_client.url
            }
        
        try:
            # Start browsing from the root node
            root_node = opc_client.client.get_node("i=84")  # Root node
            nodes_found = []
            
            # Browse children of the root node
            children = await root_node.get_children()
            
            for child in children[:max_nodes]:
                try:
                    browse_name = await child.read_browse_name()
                    node_id = child.nodeid.to_string()
                    
                    # Try to get additional info if it's a variable
                    try:
                        value = await child.get_value()
                        node_type = "Variable"
                    except:
                        node_type = "Object"
                    
                    nodes_found.append({
                        "node_id": node_id,
                        "browse_name": str(browse_name.Name),
                        "type": node_type,
                        "value": str(value) if node_type == "Variable" else None
                    })
                except Exception as e:
                    # Skip nodes we can't read
                    continue
            
            return {
                "server_url": opc_client.url,
                "total_nodes_found": len(nodes_found),
                "max_nodes_requested": max_nodes,
                "nodes": nodes_found,
                "message": f"Found {len(nodes_found)} nodes in OPC UA server"
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "message": f"Error browsing OPC UA server: {e}",
                "server_url": opc_client.url
            }
            
    except Exception as e:
        logger.error(f"Error browsing nodes: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/browse-path/{node_path:path}")
async def browse_node_path(
    node_path: str,
    context: dict = Depends(get_plant_context),
    max_children: int = 20
):
    """Browse a specific node path in the OPC UA server"""
    try:
        opc_client = get_opc_ua_client()
        
        if not opc_client.connected:
            return {
                "error": "OPC UA client is not connected",
                "message": "Cannot browse nodes - OPC UA client is not connected",
                "server_url": opc_client.url
            }
        
        try:
            # Try to get the node from the path
            node = opc_client.client.get_node(node_path)
            browse_name = await node.read_browse_name()
            
            # Get children of this node
            children = await node.get_children()
            child_nodes = []
            
            for child in children[:max_children]:
                try:
                    child_browse_name = await child.read_browse_name()
                    child_node_id = child.nodeid.to_string()
                    
                    # Try to get additional info if it's a variable
                    try:
                        value = await child.get_value()
                        node_type = "Variable"
                    except:
                        node_type = "Object"
                    
                    child_nodes.append({
                        "node_id": child_node_id,
                        "browse_name": str(child_browse_name.Name),
                        "type": node_type,
                        "value": str(value) if node_type == "Variable" else None
                    })
                except Exception as e:
                    # Skip nodes we can't read
                    continue
            
            return {
                "server_url": opc_client.url,
                "requested_path": node_path,
                "current_node": {
                    "node_id": node.nodeid.to_string(),
                    "browse_name": str(browse_name.Name),
                    "has_children": len(children) > 0
                },
                "children_found": len(child_nodes),
                "max_children_requested": max_children,
                "children": child_nodes,
                "message": f"Browsed path {node_path}, found {len(child_nodes)} children"
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "message": f"Error browsing path {node_path}: {e}",
                "server_url": opc_client.url,
                "requested_path": node_path
            }
            
    except Exception as e:
        logger.error(f"Error browsing node path: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 
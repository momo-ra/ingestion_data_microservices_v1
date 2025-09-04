from fastapi import APIRouter, HTTPException, Path, Depends, Query
from queries.timeseries_queries import get_latest_node_data, get_node_data_history, get_node_data_statistics
from queries.polling_queries import get_active_polling_tasks
from queries.tag_queries import get_tag_by_name
from schemas.schema import NodeRequest, TimeRangeRequest
from database import get_plant_db
from datetime import datetime, timedelta
from utils.log import setup_logger
from utils.response import success_response, fail_response
from routers.common_routers import get_context_with_defaults, get_plant_context
from middleware import authenticate_user, RequirePermission, Permissions, get_user_id
from services.datasource_connection_manager import get_datasource_connection_manager

router = APIRouter(prefix="/data", tags=["data"])
logger = setup_logger(__name__)

# New datasource-aware endpoints
@router.post("/read/{data_source_id}")
async def read_node_data(
    data_source_id: int = Path(..., description="The datasource ID"),
    request: NodeRequest = None,
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.VIEW_PLANT_DATA))
):
    """Read real-time data from a specific node in a datasource - requires view permission"""
    try:
        user_id = get_user_id(auth_data)
        node_id = request.node_id if request else None
        
        if not node_id:
            raise HTTPException(status_code=400, detail="node_id is required")
            
        logger.info(f"User {user_id} reading data for node {node_id} from datasource {data_source_id} in plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            # Get the datasource connection manager
            connection_manager = get_datasource_connection_manager()
            
            # Read the node data using node_id as connection_string
            result = await connection_manager.read_node(session, data_source_id, int(context["plant_id"]), node_id)
            
            return success_response(
                data={
                    "node_id": node_id,
                    "connection_string": node_id,  # In this case, node_id is used as connection_string
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "result": result
                },
                message=f"Successfully read data for node {node_id} from datasource {data_source_id}"
            )
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reading node data: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "datasource_error",
                    "node_id": node_id if 'node_id' in locals() else None,
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": f"Error reading data from datasource: {str(e)}"
            }
        )

@router.post("/read-with-connection/{data_source_id}")
async def read_node_data_with_connection_string(
    data_source_id: int = Path(..., description="The datasource ID"),
    name: str = Query(..., description="The tag name"),
    connection_string: str = Query(..., description="The connection string (node ID) to search in datasource"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.VIEW_PLANT_DATA))
):
    """Read real-time data from a specific node using connection string - requires view permission"""
    try:
        user_id = get_user_id(auth_data)
        
        if not name:
            raise HTTPException(status_code=400, detail="name is required")
            
        if not connection_string:
            raise HTTPException(status_code=400, detail="connection_string is required")
            
        logger.info(f"User {user_id} reading data for name {name} with connection_string {connection_string} from datasource {data_source_id} in plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            # Get the datasource connection manager
            connection_manager = get_datasource_connection_manager()
            
            # Read the node data using connection_string
            result = await connection_manager.read_node(session, data_source_id, int(context["plant_id"]), connection_string)
            
            return success_response(
                data={
                    "name": name,
                    "connection_string": connection_string,
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "result": result
                },
                message=f"Successfully read data for name {name} with connection_string {connection_string} from datasource {data_source_id}"
            )
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reading node data with connection string: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "datasource_error",
                    "name": name if 'name' in locals() else None,
                    "connection_string": connection_string if 'connection_string' in locals() else None,
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": f"Error reading data from datasource: {str(e)}"
            }
        )

@router.post("/read-multiple/{data_source_id}")
async def read_multiple_nodes(
    data_source_id: int = Path(..., description="The datasource ID"),
    node_ids: str = Query(..., description="Comma-separated list of node IDs to read"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.VIEW_PLANT_DATA))
):
    """Read real-time data from multiple nodes in a datasource - requires view permission"""
    try:
        user_id = get_user_id(auth_data)
        # Convert comma-separated string to list
        node_ids_list = [node_id.strip() for node_id in node_ids.split(",")]
        logger.info(f"User {user_id} reading data for {len(node_ids_list)} nodes from datasource {data_source_id} in plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            # Get the datasource connection manager
            connection_manager = get_datasource_connection_manager()
            
            # Read multiple nodes
            results = await connection_manager.read_nodes(session, data_source_id, int(context["plant_id"]), node_ids_list)
            
            return success_response(
                data={
                    "node_ids": node_ids_list,
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "results": results
                },
                message=f"Successfully read data for {len(node_ids_list)} nodes from datasource {data_source_id}"
            )
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reading multiple nodes: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "datasource_error",
                    "node_ids": node_ids_list,
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": f"Error reading data from datasource: {str(e)}"
            }
        )

@router.post("/write/{data_source_id}")
async def write_node_data(
    data_source_id: int = Path(..., description="The datasource ID"),
    node_id: str = Query(..., description="The node ID to write to"),
    value: str = Query(..., description="The value to write"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.UPDATE_PLANT_DATA))
):
    """Write data to a specific node in a datasource - requires update permission"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} writing value {value} to node {node_id} in datasource {data_source_id} for plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            # Get the datasource connection manager
            connection_manager = get_datasource_connection_manager()
            
            # Write the node data
            success = await connection_manager.write_node(session, data_source_id, int(context["plant_id"]), node_id, value)
            
            if success:
                return success_response(
                    data={
                        "node_id": node_id,
                        "value": value,
                        "data_source_id": data_source_id,
                        "plant_id": context["plant_id"]
                    },
                    message=f"Successfully wrote value {value} to node {node_id} in datasource {data_source_id}"
                )
            else:
                return fail_response(
                    message=f"Failed to write value {value} to node {node_id} in datasource {data_source_id}",
                    data={
                        "node_id": node_id,
                        "value": value,
                        "data_source_id": data_source_id,
                        "plant_id": context["plant_id"]
                    }
                )
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error writing node data: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "datasource_error",
                    "node_id": node_id,
                    "value": value,
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": f"Error writing data to datasource: {str(e)}"
            }
        )

@router.post("/query/{data_source_id}")
async def query_datasource(
    data_source_id: int = Path(..., description="The datasource ID"),
    sql: str = Query(..., description="SQL query to execute"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.VIEW_PLANT_DATA))
):
    """Execute a query on a database datasource - requires view permission"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} executing query on datasource {data_source_id} in plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            # Get the datasource connection manager
            connection_manager = get_datasource_connection_manager()
            
            # Execute the query (no params for now)
            result = await connection_manager.query(session, data_source_id, int(context["plant_id"]), sql, {})
            
            return success_response(
                data={
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "sql": sql,
                    "result": result
                },
                message=f"Successfully executed query on datasource {data_source_id}"
            )
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "datasource_error",
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": f"Error executing query on datasource: {str(e)}"
            }
        )

@router.get("/test-connection/{data_source_id}")
async def test_datasource_connection(
    data_source_id: int = Path(..., description="The datasource ID"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.VIEW_PLANT_DATA))
):
    """Test connection to a specific datasource - requires view permission"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} testing connection to datasource {data_source_id} in plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            # Get the datasource connection manager
            connection_manager = get_datasource_connection_manager()
            
            # Test the connection
            result = await connection_manager.test_connection(session, data_source_id, int(context["plant_id"]))
            
            return success_response(
                data={
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "connection_status": result
                },
                message=f"Connection test completed for datasource {data_source_id}"
            )
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing connection: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "datasource_error",
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": f"Error testing datasource connection: {str(e)}"
            }
        )

# Legacy endpoints (existing functionality)
@router.post("/latest")
async def get_latest_data(
    request: NodeRequest,
    context: dict = Depends(get_context_with_defaults),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.VIEW_PLANT_DATA))
):
    """Get the latest data for a specific node - requires view permission"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting latest data for node {request.node_id} in plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            data = await get_latest_node_data(
                session,
                request.node_id,
                context["plant_id"]
            )
            if data:
                return success_response(
                    data=data,
                    message=f"Retrieved latest data for node {request.node_id}"
                )
            else:
                return fail_response(
                    message=f"No data found for node {request.node_id}",
                    data={"node_id": request.node_id, "plant_id": context["plant_id"]}
                )
            break
    except Exception as e:
        logger.error(f"Error getting latest data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/history")
async def get_data_history(
    request: TimeRangeRequest,
    context: dict = Depends(get_context_with_defaults),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.VIEW_PLANT_DATA))
):
    """Get historical data for a specific node - requires view permission"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting data history for node {request.node_id} in plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            data = await get_node_data_history(
                session,
                request.node_id,
                context["plant_id"],
                start_time=request.start_time,
                end_time=request.end_time,
                limit=request.limit or 100
            )
            return success_response(
                data={
                    "node_id": request.node_id, 
                    "data": data, 
                    "count": len(data),
                    "plant_id": context["plant_id"]
                },
                message=f"Retrieved {len(data)} historical records for node {request.node_id}"
            )
            break
    except Exception as e:
        logger.error(f"Error getting data history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/statistics")
async def get_data_statistics(
    request: TimeRangeRequest,
    context: dict = Depends(get_context_with_defaults),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.VIEW_PLANT_DATA))
):
    """Get statistics for a specific node's data - requires view permission"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting statistics for node {request.node_id} in plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            stats = await get_node_data_statistics(
                session,
                request.node_id,
                context["plant_id"],
                start_time=request.start_time,
                end_time=request.end_time
            )
            return success_response(
                data=stats,
                message=f"Retrieved statistics for node {request.node_id}"
            )
            break
    except Exception as e:
        logger.error(f"Error getting data statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/recent/{hours}")
async def get_recent_data(
    hours: int = Path(..., ge=1, le=24),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.VIEW_PLANT_DATA))
):
    """Get recent data for all active polling nodes (plant-level) - requires view permission"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting recent data for {hours} hours in plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            # Get all active polling tasks (plant-level)
            tasks = await get_active_polling_tasks(
                session,
                context["plant_id"]
            )
            
            # Calculate time range
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            # Get data for each node
            results = []
            for task in tasks:
                try:
                    # Use tag_name directly as node_id
                    node_id = task['tag_name']
                    
                    # Get data history (plant-level, no workspace)
                    data = await get_node_data_history(
                        session,
                        node_id,
                        context["plant_id"],
                        start_time=start_time,
                        end_time=end_time,
                        limit=100
                    )
                    
                    # Get statistics (plant-level, no workspace)
                    stats = await get_node_data_statistics(
                        session,
                        node_id,
                        context["plant_id"],
                        start_time=start_time,
                        end_time=end_time
                    )
                    
                    results.append({
                        "node_id": node_id,
                        "tag_name": task["tag_name"],
                        "interval_seconds": task["interval_seconds"],
                        "data_count": len(data),
                        "statistics": stats,
                        "latest_data": data[0] if data else None
                    })
                except Exception as node_error:
                    logger.error(f"Error getting data for node {task['tag_name']}: {node_error}")
                    results.append({
                        "node_id": task['tag_name'],
                        "tag_name": task["tag_name"],
                        "error": str(node_error)
                    })
            
            return success_response(
                data={
                    "time_range": {
                        "start": start_time.isoformat(),
                        "end": end_time.isoformat(),
                        "hours": hours
                    },
                    "nodes": results,
                    "count": len(results),
                    "plant_id": context["plant_id"]
                },
                message=f"Retrieved recent data for {len(results)} nodes over {hours} hours"
            )
            break
    except Exception as e:
        logger.error(f"Error getting recent data: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 
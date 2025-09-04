from fastapi import APIRouter, HTTPException, Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession
from services.tag_services import (
    get_or_create_tag,
    create_tag_service,
    get_tag_by_id_service,
    get_tag_by_name_service,
    update_tag_service,
    delete_tag_service,
    get_all_tags_service,
    get_active_tags_service
)
from database import get_plant_db, get_central_db
from utils.log import setup_logger
from utils.response import success_response, fail_response
from routers.common_routers import get_plant_context, DEFAULT_PLANT_ID
from middleware import authenticate_user, RequirePermission, Permissions, get_user_id
from schemas.schema import TagCreateRequest, TagUpdateRequest
from sqlalchemy import text

router = APIRouter(prefix="/tags", tags=["tags"])
logger = setup_logger(__name__)

async def require_plant_data_permission(
    auth_data: dict = Depends(authenticate_user),
    db: AsyncSession = Depends(get_central_db)
):
    """Custom permission dependency that allows system_admin or plant data permissions"""
    user_id = get_user_id(auth_data)
    
    try:
        # Check if user has system_admin permission
        query = text("""
            SELECT DISTINCT gp.name 
            FROM global_permissions gp
            JOIN global_role_permissions grp ON gp.id = grp.permission_id
            JOIN global_roles gr ON grp.role_id = gr.id
            JOIN user_plant_access upa ON upa.global_role_id = gr.id
            WHERE upa.user_id = :user_id 
            AND upa.is_active = true
            AND gp.name IN ('system_admin', 'view_plant_data', 'create_plant_data', 'update_plant_data', 'delete_plant_data')
        """)
        result = await db.execute(query, {"user_id": user_id})
        permissions = [row[0] for row in result.all()]
        
        if 'system_admin' in permissions:
            logger.info(f"User {user_id} has system_admin permission - allowing access")
            return {"user_id": user_id, "permissions": permissions}
        
        if any(perm in permissions for perm in ['view_plant_data', 'create_plant_data', 'update_plant_data', 'delete_plant_data']):
            logger.info(f"User {user_id} has plant data permissions: {permissions}")
            return {"user_id": user_id, "permissions": permissions}
        
        logger.warning(f"User {user_id} lacks required permissions for plant data access")
        raise HTTPException(
            status_code=403, 
            detail={
                "status": "fail",
                "data": {
                    "error_type": "permission_denied",
                    "user_id": user_id,
                    "required_permissions": ["system_admin", "view_plant_data", "create_plant_data", "update_plant_data", "delete_plant_data"]
                },
                "message": "Forbidden: Insufficient permissions for plant data access"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking plant data permissions for user {user_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {"error_type": "permission_check_error"},
                "message": "Error checking permissions"
            }
        )

@router.get("/")
async def get_all_tags(
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of tags to return"),
    offset: int = Query(0, ge=0, description="Number of tags to skip")
):
    """Get all tags from the plant database with pagination - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting all tags from plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await get_all_tags_service(session, context["plant_id"], limit, offset)
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting all tags: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving tags"
            }
        )

@router.get("/active")
async def get_active_tags(
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Get all active tags from the plant database - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting active tags from plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await get_active_tags_service(session, context["plant_id"])
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting active tags: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving active tags"
            }
        )

@router.get("/{tag_id}")
async def get_tag_by_id(
    tag_id: int = Path(..., description="The tag ID"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Get tag by ID from the plant database - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting tag {tag_id} from plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await get_tag_by_id_service(session, tag_id, context["plant_id"])
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting tag by ID: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "tag_id": tag_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving tag"
            }
        )

@router.get("/name/{tag_name}")
async def get_tag_by_name(
    tag_name: str = Path(..., description="The tag name"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Get tag by name from the plant database - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting tag '{tag_name}' from plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await get_tag_by_name_service(session, tag_name, context["plant_id"])
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting tag by name: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "tag_name": tag_name,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving tag by name"
            }
        )

@router.post("/")
async def create_tag(
    request: TagCreateRequest,
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Create a new tag in the plant database - requires create permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} creating tag '{request.name}' with connection_string '{request.connection_string}' in datasource {request.data_source_id} for plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await create_tag_service(session, request.name, context["plant_id"], request.data_source_id, request.connection_string)
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating tag: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "node_id": request.node_id,
                    "data_source_id": request.data_source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while creating tag"
            }
        )

@router.put("/{tag_id}")
async def update_tag(
    tag_id: int = Path(..., description="The tag ID"),
    request: TagUpdateRequest = None,
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Update a tag in the plant database - requires update permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} updating tag {tag_id} in plant {context['plant_id']}")
        
        # Prepare update data
        update_data = {}
        if request:
            if request.name is not None:
                update_data["name"] = request.name
            if request.description is not None:
                update_data["description"] = request.description
            if request.unit_of_measure is not None:
                update_data["unit_of_measure"] = request.unit_of_measure
            if request.is_active is not None:
                update_data["is_active"] = request.is_active
        
        if not update_data:
            return fail_response(
                message="No update data provided",
                data={
                    "error_type": "validation_error",
                    "tag_id": tag_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                }
            )
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await update_tag_service(session, tag_id, context["plant_id"], **update_data)
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating tag: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "tag_id": tag_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while updating tag"
            }
        )

@router.delete("/{tag_id}")
async def delete_tag(
    tag_id: int = Path(..., description="The tag ID"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Delete a tag from the plant database - requires delete permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} deleting tag {tag_id} from plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await delete_tag_service(session, tag_id, context["plant_id"])
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting tag: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "tag_id": tag_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while deleting tag"
            }
        )

# New datasource-aware endpoint
@router.get("/node/{name}/datasource/{data_source_id}")
async def get_or_create_tag_with_datasource(
    name: str = Path(..., description="The tag name"),
    data_source_id: int = Path(..., description="The datasource ID"),
    connection_string: str = Query(None, description="The connection string (node ID) to search in datasource"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Get or create tag by name and datasource ID - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        # Use connection_string if provided, otherwise use name
        actual_connection_string = connection_string if connection_string else name
        logger.info(f"User {user_id} requesting tag for name {name} with connection_string {actual_connection_string} in datasource {data_source_id} for plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            tag_id = await get_or_create_tag(session, name, context["plant_id"], data_source_id, actual_connection_string)
            if tag_id:
                return success_response(
                    data={
                        "tag_id": tag_id, 
                        "name": name,
                        "connection_string": actual_connection_string,
                        "data_source_id": data_source_id,
                        "plant_id": context["plant_id"]
                    },
                    message=f"Successfully got or created tag for name {name} with connection_string {actual_connection_string} in datasource {data_source_id}"
                )
            else:
                return fail_response(
                    message=f"Failed to get or create tag for name {name} with connection_string {actual_connection_string} in datasource {data_source_id}",
                    data={
                        "error_type": "tag_creation_failed",
                        "name": name,
                        "connection_string": actual_connection_string,
                        "data_source_id": data_source_id,
                        "plant_id": context["plant_id"],
                        "user_id": user_id
                    }
                )
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting or creating tag: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "node_id": node_id,
                    "data_source_id": data_source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while getting or creating tag"
            }
        )

# Legacy endpoint for backward compatibility (deprecated)
@router.get("/node/{name}")
async def get_or_create_tag_legacy(
    name: str = Path(..., description="The tag name (deprecated - use /node/{name}/datasource/{data_source_id})"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Get or create tag by name (legacy endpoint - deprecated) - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.warning(f"User {user_id} using deprecated endpoint for name {name} in plant {context['plant_id']}")
        
        # For backward compatibility, we'll try to find a default OPC UA datasource
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            # Try to find a default OPC UA datasource
            from queries.datasource_queries import get_data_sources_by_type
            from queries.datasource_queries import get_data_source_type_by_name
            
            # Get OPC UA type
            opcua_type = await get_data_source_type_by_name(session, "opcua")
            if not opcua_type:
                return fail_response(
                    message="No OPC UA datasource type found. Please use the new endpoint with explicit datasource ID.",
                    data={
                        "error_type": "deprecated_endpoint",
                        "node_id": node_id,
                        "plant_id": context["plant_id"],
                        "user_id": user_id,
                        "suggestion": "Use /node/{node_id}/datasource/{data_source_id} endpoint"
                    }
                )
            
            # Get first active OPC UA datasource
            opcua_sources = await get_data_sources_by_type(session, opcua_type.id, int(context["plant_id"]), active_only=True)
            if not opcua_sources:
                return fail_response(
                    message="No active OPC UA datasource found. Please use the new endpoint with explicit datasource ID.",
                    data={
                        "error_type": "deprecated_endpoint",
                        "node_id": node_id,
                        "plant_id": context["plant_id"],
                        "user_id": user_id,
                        "suggestion": "Use /node/{node_id}/datasource/{data_source_id} endpoint"
                    }
                )
            
            # Use the first OPC UA datasource
            default_data_source_id = opcua_sources[0].id
            tag_id = await get_or_create_tag(session, node_id, context["plant_id"], default_data_source_id)
            if tag_id:
                return success_response(
                    data={
                        "tag_id": tag_id, 
                        "node_id": node_id, 
                        "data_source_id": default_data_source_id,
                        "plant_id": context["plant_id"],
                        "warning": "Using deprecated endpoint with default OPC UA datasource"
                    },
                    message=f"Successfully got or created tag for node {node_id} using default OPC UA datasource"
                )
            else:
                return fail_response(
                    message=f"Failed to get or create tag for node {node_id}",
                    data={
                        "error_type": "tag_creation_failed",
                        "node_id": node_id,
                        "plant_id": context["plant_id"],
                        "user_id": user_id
                    }
                )
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting or creating tag: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "node_id": node_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while getting or creating tag"
            }
        )
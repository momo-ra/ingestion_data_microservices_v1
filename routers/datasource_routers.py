from fastapi import APIRouter, HTTPException, Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession
from services.datasource_services import (
    # DataSource Type services
    create_data_source_type_service,
    get_data_source_type_by_id_service,
    get_data_source_type_by_name_service,
    get_all_data_source_types_service,
    update_data_source_type_service,
    delete_data_source_type_service,
    # DataSource services
    create_data_source_service,
    get_data_source_by_id_service,
    get_data_source_by_name_service,
    get_all_data_sources_service,
    get_active_data_sources_service,
    update_data_source_service,
    delete_data_source_service,
    get_data_sources_by_type_service,
    test_data_source_connection_config_service
)
from services.tag_services import get_tags_by_data_source_service
from database import get_plant_db, get_central_db
from utils.log import setup_logger
from utils.response import success_response, fail_response
from routers.common_routers import get_plant_context, DEFAULT_PLANT_ID
from middleware import authenticate_user, RequirePermission, Permissions, get_user_id
from schemas.schema import (
    DataSourceTypeCreateRequest, 
    DataSourceTypeUpdateRequest,
    DataSourceCreateRequest, 
    DataSourceUpdateRequest,
    DataSourceTestConnectionRequest
)
from sqlalchemy import text

router = APIRouter(prefix="/datasources", tags=["datasources"])
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

# =============================================================================
# DATA SOURCE TYPE ENDPOINTS
# =============================================================================

@router.get("/types")
async def get_all_data_source_types(
    context: dict = Depends(get_plant_context),
    active_only: bool = Query(True, description="Return only active types"),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Get all data source types - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting all data source types from plant {context['plant_id']}")
        
        # Use plant database for data source types
        async for session in get_plant_db(context["plant_id"]):
            response = await get_all_data_source_types_service(session, active_only)
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting all data source types: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving data source types"
            }
        )

@router.get("/types/{type_id}")
async def get_data_source_type_by_id(
    type_id: int = Path(..., description="The data source type ID"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Get data source type by ID - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting data source type {type_id} from plant {context['plant_id']}")
        
        # Use plant database for data source types
        async for session in get_plant_db(context["plant_id"]):
            response = await get_data_source_type_by_id_service(session, type_id)
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data source type by ID: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "type_id": type_id,
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving data source type"
            }
        )

@router.get("/types/name/{type_name}")
async def get_data_source_type_by_name(
    type_name: str = Path(..., description="The data source type name"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Get data source type by name - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting data source type '{type_name}' from plant {context['plant_id']}")
        
        # Use plant database for data source types
        async for session in get_plant_db(context["plant_id"]):
            response = await get_data_source_type_by_name_service(session, type_name)
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data source type by name: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "type_name": type_name,
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving data source type"
            }
        )

@router.post("/types")
async def create_data_source_type(
    request: DataSourceTypeCreateRequest,
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Create a new data source type - requires create permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} creating data source type '{request.name}' in plant {context['plant_id']}")
        
        # Use plant database for data source types
        async for session in get_plant_db(context["plant_id"]):
            response = await create_data_source_type_service(
                session, 
                request.name, 
                request.description
            )
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating data source type: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "user_id": user_id,
                    "request_data": request.dict()
                },
                "message": "Internal server error while creating data source type"
            }
        )

@router.put("/types/{type_id}")
async def update_data_source_type(
    type_id: int = Path(..., description="The data source type ID"),
    request: DataSourceTypeUpdateRequest = None,
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Update a data source type - requires update permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} updating data source type {type_id} in plant {context['plant_id']}")
        
        if not request:
            raise HTTPException(
                status_code=400,
                detail={
                    "status": "fail",
                    "data": {"error_type": "missing_request_body"},
                    "message": "Request body is required for update operations"
                }
            )
        
        # Use plant database for data source types
        async for session in get_plant_db(context["plant_id"]):
            update_data = {k: v for k, v in request.dict().items() if v is not None}
            response = await update_data_source_type_service(session, type_id, **update_data)
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating data source type: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "type_id": type_id,
                    "user_id": user_id,
                    "request_data": request.dict() if request else None
                },
                "message": "Internal server error while updating data source type"
            }
        )

@router.delete("/types/{type_id}")
async def delete_data_source_type(
    type_id: int = Path(..., description="The data source type ID"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Delete a data source type (soft delete) - requires delete permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} deleting data source type {type_id} from plant {context['plant_id']}")
        
        # Use plant database for data source types
        async for session in get_plant_db(context["plant_id"]):
            response = await delete_data_source_type_service(session, type_id)
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting data source type: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "type_id": type_id,
                    "user_id": user_id
                },
                "message": "Internal server error while deleting data source type"
            }
        )

# =============================================================================
# DATA SOURCE ENDPOINTS
# =============================================================================

@router.get("/")
async def get_all_data_sources(
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission),
    active_only: bool = Query(True, description="Return only active data sources"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of data sources to return"),
    offset: int = Query(0, ge=0, description="Number of data sources to skip"),
    sort_by: str = Query("name", description="Field to sort by (name, created_at, updated_at, type_name)"),
    sort_direction: str = Query("asc", description="Sort direction (asc or desc)")
):
    """Get all data sources from the plant database with pagination - requires view permission or system_admin"""
    try:
        # Validate sort parameters
        valid_sort_fields = ["name", "created_at", "updated_at", "type_name"]
        if sort_by not in valid_sort_fields:
            raise HTTPException(
                status_code=400,
                detail={
                    "status": "fail",
                    "data": {
                        "error_type": "invalid_sort_field",
                        "sort_by": sort_by,
                        "valid_fields": valid_sort_fields
                    },
                    "message": f"Invalid sort field. Must be one of: {', '.join(valid_sort_fields)}"
                }
            )
        
        valid_sort_directions = ["asc", "desc"]
        if sort_direction.lower() not in valid_sort_directions:
            raise HTTPException(
                status_code=400,
                detail={
                    "status": "fail",
                    "data": {
                        "error_type": "invalid_sort_direction",
                        "sort_direction": sort_direction,
                        "valid_directions": valid_sort_directions
                    },
                    "message": f"Invalid sort direction. Must be one of: {', '.join(valid_sort_directions)}"
                }
            )
        
        user_id = get_user_id(auth_data)
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting all data sources from plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await get_all_data_sources_service(session, int(context["plant_id"]), active_only, limit, offset, sort_by, sort_direction)
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting all data sources: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving data sources"
            }
        )

@router.get("/active")
async def get_active_data_sources(
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Get all active data sources from the plant database - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting active data sources from plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await get_active_data_sources_service(session, int(context["plant_id"]))
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting active data sources: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving active data sources"
            }
        )

@router.get("/{source_id}")
async def get_data_source_by_id(
    source_id: int = Path(..., description="The data source ID"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Get data source by ID from the plant database - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting data source {source_id} from plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await get_data_source_by_id_service(session, source_id, int(context["plant_id"]))
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data source by ID: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "source_id": source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving data source"
            }
        )

@router.get("/name/{source_name}")
async def get_data_source_by_name(
    source_name: str = Path(..., description="The data source name"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Get data source by name from the plant database - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting data source '{source_name}' from plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await get_data_source_by_name_service(session, source_name, int(context["plant_id"]))
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data source by name: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "source_name": source_name,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving data source"
            }
        )

@router.get("/type/{type_id}")
async def get_data_sources_by_type(
    type_id: int = Path(..., description="The data source type ID"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission),
    active_only: bool = Query(True, description="Return only active data sources")
):
    """Get data sources by type from the plant database - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} requesting data sources of type {type_id} from plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await get_data_sources_by_type_service(session, type_id, int(context["plant_id"]), active_only)
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data sources by type: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "type_id": type_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while retrieving data sources by type"
            }
        )

@router.post("/")
async def create_data_source(
    request: DataSourceCreateRequest,
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Create a new data source in the plant database - requires create permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} creating data source '{request.name}' in plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await create_data_source_service(
                session,
                request.name,
                request.type_id,
                int(context["plant_id"]),
                request.description,
                request.connection_config
            )
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating data source: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "plant_id": context["plant_id"],
                    "user_id": user_id,
                    "request_data": request.dict()
                },
                "message": "Internal server error while creating data source"
            }
        )

@router.put("/{source_id}")
async def update_data_source(
    source_id: int = Path(..., description="The data source ID"),
    request: DataSourceUpdateRequest = None,
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Update a data source in the plant database - requires update permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} updating data source {source_id} in plant {context['plant_id']}")
        
        if not request:
            raise HTTPException(
                status_code=400,
                detail={
                    "status": "fail",
                    "data": {"error_type": "missing_request_body"},
                    "message": "Request body is required for update operations"
                }
            )
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            update_data = {k: v for k, v in request.dict().items() if v is not None}
            response = await update_data_source_service(session, source_id, int(context["plant_id"]), **update_data)
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating data source: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "source_id": source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id,
                    "request_data": request.dict() if request else None
                },
                "message": "Internal server error while updating data source"
            }
        )

@router.delete("/{source_id}")
async def delete_data_source(
    source_id: int = Path(..., description="The data source ID"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Delete a data source (soft delete) from the plant database - requires delete permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} deleting data source {source_id} from plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await delete_data_source_service(session, source_id, int(context["plant_id"]))
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting data source: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "source_id": source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while deleting data source"
            }
        )

@router.post("/test-connection")
async def test_data_source_connection(
    request: DataSourceTestConnectionRequest,
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission)
):
    """Test data source connection configuration without saving to database - requires create permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} testing data source connection for type {request.type_id} in plant {context['plant_id']}")
        
        # Use plant database for data source types (they are plant-specific)
        async for session in get_plant_db(context["plant_id"]):
            response = await test_data_source_connection_config_service(
                session,
                request.type_id,
                request.connection_config
            )
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing data source connection: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "connection_test_error",
                    "type_id": request.type_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id,
                    "connection_config": request.connection_config
                },
                "message": "Internal server error while testing data source connection"
            }
        ) 

@router.get("/{source_id}/tags/explore")
async def explore_data_source_tags(
    source_id: int = Path(..., description="The data source ID"),
    context: dict = Depends(get_plant_context),
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(require_plant_data_permission),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of tags to return"),
    offset: int = Query(0, ge=0, description="Number of tags to skip")
):
    """Explore tags for a specific data source - requires view permission or system_admin"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} exploring tags for data source {source_id} in plant {context['plant_id']}")
        
        # Get database session for the plant
        async for session in get_plant_db(context["plant_id"]):
            response = await get_tags_by_data_source_service(
                session, 
                source_id, 
                context["plant_id"], 
                limit, 
                offset
            )
            return response
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exploring tags for data source {source_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "fail",
                "data": {
                    "error_type": "database_error",
                    "source_id": source_id,
                    "plant_id": context["plant_id"],
                    "user_id": user_id
                },
                "message": "Internal server error while exploring data source tags"
            }
        )
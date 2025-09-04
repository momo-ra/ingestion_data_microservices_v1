"""
Middleware package for authentication and authorization.

This package contains middleware components for:
- JWT authentication (auth_middleware.py)
- Permission-based authorization (permission_middleware.py)
"""

from .auth_middleware import (
    authenticate_user,
    verify_token,
    verify_ws_token,
    authenticate_ws,
    get_user_id,
    is_admin
)

from .permission_middleware import (
    RequirePermission,
    RequireWorkspaceAccess,
    RequireWorkspaceOwnership,
    check_global_permission,
    check_permission,
    get_user_permissions,
    get_user_global_permissions,
    Permissions
)

__all__ = [
    # Auth middleware exports
    "authenticate_user",
    "verify_token", 
    "verify_ws_token",
    "authenticate_ws",
    "get_user_id",
    "is_admin",
    
    # Permission middleware exports
    "RequirePermission",
    "RequireWorkspaceAccess", 
    "RequireWorkspaceOwnership",
    "check_global_permission",
    "check_permission",
    "get_user_permissions",
    "get_user_global_permissions",
    "Permissions"
] 
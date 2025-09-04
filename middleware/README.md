# Authentication and Authorization Middleware

This directory contains middleware components for handling authentication and authorization in the OPC-UA Data Ingestion Microservice.

## Components

### 1. Authentication Middleware (`auth_middleware.py`)

Handles JWT token authentication for both HTTP requests and WebSocket connections.

#### Features:
- JWT token verification
- HTTP Bearer token authentication
- WebSocket token authentication
- User role checking
- Helper functions for extracting user information

#### Usage:

```python
from middleware import authenticate_user, get_user_id, is_admin

# Protect an endpoint with authentication
@router.get("/protected")
async def protected_endpoint(auth_data: dict = Depends(authenticate_user)):
    user_id = get_user_id(auth_data)
    if is_admin(auth_data):
        # Admin-specific logic
        pass
    return {"message": "Authenticated", "user_id": user_id}
```

#### WebSocket Authentication:

```python
from middleware import authenticate_ws

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    # Authenticate WebSocket connection
    auth_data = await authenticate_ws(websocket)
    if not auth_data:
        return  # WebSocket will be closed automatically
    
    # Handle authenticated WebSocket communication
    user_id = get_user_id(auth_data)
    # ... rest of WebSocket logic
```

### 2. Permission Middleware (`permission_middleware.py`)

Handles permission-based authorization using database-stored permissions.

#### Features:
- Global permission checking
- Plant-specific permission checking
- Workspace access control
- Card ownership validation
- First-time system access handling
- Role-based permission management

#### Available Permissions:

```python
from middleware import Permissions

# Common permission names
Permissions.VIEW_ANY_USER_CARDS = "view_any_user_cards"
Permissions.CREATE_ANY_USER_CARDS = "create_any_user_cards"
Permissions.DELETE_ANY_USER_CARDS = "delete_any_user_cards"
Permissions.EDIT_ANY_USER_CARDS = "edit_any_user_cards"
Permissions.ADMIN_ACCESS = "admin_access"
Permissions.VIEW_PLANT_DATA = "view_plant_data"
Permissions.EDIT_PLANT_DATA = "edit_plant_data"
Permissions.MANAGE_USERS = "manage_users"
Permissions.MANAGE_WORKSPACES = "manage_workspaces"
```

#### Usage:

```python
from middleware import RequirePermission, Permissions

# Protect endpoint with specific permission
@router.get("/admin/data")
async def admin_data_endpoint(
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.ADMIN_ACCESS))
):
    return {"message": "Admin data", "user_id": get_user_id(auth_data)}

# Protect endpoint with plant data permission
@router.get("/plant/data")
async def plant_data_endpoint(
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.VIEW_PLANT_DATA))
):
    return {"message": "Plant data accessible"}
```

#### Workspace Access Control:

```python
from middleware import RequireWorkspaceAccess, RequireWorkspaceOwnership

# Check workspace access
@router.get("/workspace/{workspace_id}/data")
async def workspace_data_endpoint(
    workspace_id: int,
    plant_id: str,
    auth_data: dict = Depends(authenticate_user),
    access_check: dict = Depends(RequireWorkspaceAccess())
):
    return {"message": "Workspace data accessible"}

# Check workspace ownership
@router.delete("/workspace/{workspace_id}")
async def delete_workspace_endpoint(
    workspace_id: int,
    auth_data: dict = Depends(authenticate_user),
    ownership_check: dict = Depends(RequireWorkspaceOwnership())
):
    return {"message": "Workspace deleted"}
```

## Configuration

### Environment Variables

The middleware requires the following environment variables:

```bash
# JWT Configuration
JWT_SECRET=your_super_secret_jwt_key_here_make_it_long_and_random
JWT_ALGORITHM=HS256
JWT_EXPIRE_MINUTES=30
```

### Database Requirements

The permission middleware requires the following database tables:

1. **Central Database Tables:**
   - `users` - User information
   - `global_roles` - Global roles
   - `global_permissions` - Available permissions
   - `global_role_permissions` - Role-permission mappings
   - `user_plant_access` - User access to plants with roles

2. **Plant Database Tables:**
   - `workspaces` - Workspace information
   - `cards` - Card information
   - `role` - Plant-specific roles (legacy)
   - `permission` - Plant-specific permissions (legacy)
   - `role_permission` - Role-permission mappings (legacy)

## Integration with FastAPI

### Router-Level Authentication

```python
# Apply authentication to all routes in a router
app.include_router(
    my_router, 
    prefix="/api/v1", 
    dependencies=[Depends(authenticate_user)]
)
```

### Individual Endpoint Protection

```python
# Protect individual endpoints
@router.get("/public")
async def public_endpoint():
    return {"message": "Public access"}

@router.get("/protected")
async def protected_endpoint(auth_data: dict = Depends(authenticate_user)):
    return {"message": "Protected access"}

@router.get("/admin")
async def admin_endpoint(
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.ADMIN_ACCESS))
):
    return {"message": "Admin access"}
```

## Error Handling

The middleware automatically handles authentication and authorization errors:

- **401 Unauthorized**: Invalid or missing JWT token
- **403 Forbidden**: User lacks required permissions
- **404 Not Found**: User, workspace, or card not found

## Security Best Practices

1. **JWT Secret**: Use a strong, random secret key
2. **Token Expiration**: Set reasonable token expiration times
3. **HTTPS**: Always use HTTPS in production
4. **Permission Granularity**: Use specific permissions rather than broad ones
5. **Database Security**: Ensure database connections are secure
6. **Logging**: Monitor authentication and authorization events

## Testing

To test the middleware:

1. **Generate a JWT token** with the required claims (user_id, roles)
2. **Include the token** in the Authorization header: `Bearer <token>`
3. **Verify permissions** are correctly enforced
4. **Test WebSocket authentication** with token in query parameters

## Troubleshooting

### Common Issues:

1. **JWT Configuration Missing**: Ensure JWT_SECRET and JWT_ALGORITHM are set
2. **Database Connection Issues**: Check central database connectivity
3. **Permission Not Found**: Verify permissions exist in the database
4. **User Not Found**: Ensure user exists in the users table
5. **Plant Access Issues**: Check user_plant_access table for plant permissions 
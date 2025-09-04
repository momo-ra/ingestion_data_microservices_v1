# OPC-UA Data Ingestion Microservice

A FastAPI-based microservice for ingesting and managing OPC-UA data with multi-database architecture, plant-specific data management, and comprehensive authentication and authorization.

## Features

- **Multi-Database Architecture**: Central database for users/permissions + plant-specific databases for data
- **OPC-UA Integration**: Real-time data polling and subscription management
- **Authentication & Authorization**: JWT-based authentication with role-based permissions
- **Plant-Specific Data Management**: Isolated data storage per plant
- **Real-time Monitoring**: Health checks and metrics collection
- **Kafka Integration**: Optional event streaming
- **WebSocket Support**: Real-time data streaming with authentication
- **Comprehensive Logging**: Structured logging with configurable levels

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Central DB    │    │   Plant 1 DB    │    │   Plant N DB    │
│ (Users, Perms)  │    │  (Plant Data)   │    │  (Plant Data)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  FastAPI App    │
                    │  + Middleware   │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │   OPC-UA        │
                    │   Server        │
                    └─────────────────┘
```

## Quick Start

### 1. Environment Setup

Copy the environment template and configure your settings:

```bash
cp env.template .env
```

Edit `.env` with your configuration:

```bash
# Database Configuration
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=central_db

# JWT Configuration (REQUIRED for authentication)
JWT_SECRET=your_super_secret_jwt_key_here_make_it_long_and_random
JWT_ALGORITHM=HS256
JWT_EXPIRE_MINUTES=30

# OPC-UA Configuration
OPC_UA_URL=opc.tcp://localhost:4840/

# Server Configuration
PORT=8007
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Database Setup

Ensure your databases are running and accessible. The service will automatically create necessary tables on startup.

### 4. Run the Service

```bash
python main.py
```

The service will be available at `http://localhost:8007`

## Authentication & Authorization

### JWT Authentication

All protected endpoints require a valid JWT token in the Authorization header:

```bash
Authorization: Bearer <your_jwt_token>
```

### Available Permissions

- `view_plant_data` - View plant-specific data
- `edit_plant_data` - Modify plant data
- `admin_access` - Administrative access
- `manage_users` - User management
- `manage_workspaces` - Workspace management

### Example Usage

```python
# Public endpoint (no authentication required)
GET /api/v1/health

# Protected endpoint (authentication required)
GET /api/v1/user/profile
Authorization: Bearer <jwt_token>

# Admin endpoint (authentication + permission required)
GET /api/v1/admin/system-info
Authorization: Bearer <jwt_token>
```

## API Endpoints

### System Endpoints (Public)
- `GET /api/v1/` - Service status
- `GET /api/v1/health` - Health check
- `GET /api/v1/metrics` - System metrics
- `GET /api/v1/plants` - List available plants

### Protected Endpoints
- `GET /api/v1/user/profile` - User profile (authentication required)
- `GET /api/v1/admin/system-info` - Admin system info (admin permission required)

### Data Endpoints (Authentication + Permissions Required)
- `POST /api/v1/data/latest` - Get latest node data
- `POST /api/v1/data/history` - Get historical data
- `POST /api/v1/data/statistics` - Get data statistics
- `GET /api/v1/data/recent/{hours}` - Get recent data

### Node Management (Authentication Required)
- `GET /api/v1/node/` - List nodes
- `POST /api/v1/node/` - Create node
- `PUT /api/v1/node/{node_id}` - Update node
- `DELETE /api/v1/node/{node_id}` - Delete node

### Polling Management (Authentication Required)
- `GET /api/v1/node/poll/` - List polling tasks
- `POST /api/v1/node/poll/` - Create polling task
- `PUT /api/v1/node/poll/{task_id}` - Update polling task
- `DELETE /api/v1/node/poll/{task_id}` - Delete polling task

## Database Schema

### Central Database Tables
- `users` - User accounts
- `global_roles` - Global roles
- `global_permissions` - Available permissions
- `global_role_permissions` - Role-permission mappings
- `user_plant_access` - User access to plants
- `plants_registry` - Plant registry

### Plant Database Tables
- `node_data` - Time-series data
- `nodes` - Node definitions
- `polling_tasks` - Polling configuration
- `workspaces` - Workspace management
- `cards` - Card definitions

## Configuration

### Environment Variables

See `env.template` for all available configuration options.

### Key Configuration Sections:

1. **Database**: Central and plant-specific database connections
2. **JWT**: Authentication token configuration
3. **OPC-UA**: Server connection settings
4. **Logging**: Log level and output configuration
5. **Kafka**: Optional event streaming configuration

## Development

### Project Structure

```
├── config/                 # Configuration management
├── core/                   # Core application logic
├── middleware/             # Authentication & authorization
├── models/                 # Database models
├── queries/                # Database queries
├── routers/                # API route handlers
├── schemas/                # Pydantic schemas
├── services/               # Business logic services
├── utils/                  # Utility functions
├── main.py                 # Application entry point
├── database.py             # Database configuration
└── requirements.txt        # Python dependencies
```

### Adding New Endpoints

1. **Create the endpoint** in the appropriate router file
2. **Add authentication** if needed: `auth_data: dict = Depends(authenticate_user)`
3. **Add permissions** if needed: `permission_check: dict = Depends(RequirePermission(Permissions.VIEW_PLANT_DATA))`
4. **Update documentation** in the router's docstring

### Testing

```bash
# Run the service
python main.py

# Test health endpoint
curl http://localhost:8007/api/v1/health

# Test protected endpoint (requires JWT token)
curl -H "Authorization: Bearer <your_token>" http://localhost:8007/api/v1/user/profile
```

## Monitoring & Health Checks

### Health Endpoints
- `GET /api/v1/health` - Basic health status
- `GET /api/v1/health/detailed` - Detailed system information

### Metrics
- `GET /api/v1/metrics` - JSON metrics
- `GET /api/v1/metrics/prometheus` - Prometheus format metrics

### Logging
Structured logging with configurable levels. Logs include:
- Authentication events
- Database operations
- OPC-UA connection status
- Error tracking

## Security Considerations

1. **JWT Security**: Use strong, random secrets
2. **Database Security**: Secure database connections
3. **Network Security**: Use HTTPS in production
4. **Permission Granularity**: Use specific permissions
5. **Input Validation**: All inputs are validated via Pydantic schemas
6. **Error Handling**: Sensitive information is not exposed in errors

## Troubleshooting

### Common Issues

1. **JWT Configuration Missing**: Ensure JWT_SECRET and JWT_ALGORITHM are set
2. **Database Connection Issues**: Check database credentials and connectivity
3. **OPC-UA Connection Issues**: Verify OPC-UA server is running and accessible
4. **Permission Errors**: Check user permissions in the database

### Logs

Check the application logs for detailed error information. Log level can be configured via `LOG_LEVEL` environment variable.

## Contributing

1. Follow the existing code structure
2. Add appropriate authentication and permissions to new endpoints
3. Update documentation for new features
4. Test thoroughly before submitting changes

## License

[Add your license information here]

# API Routers Organization

This directory contains organized API routers for the OPC-UA Data Ingestion Microservice. The endpoints have been separated into logical modules for better organization and maintainability.

## Router Structure

### 📁 `common.py`
**Shared utilities and dependencies**
- `get_context_with_defaults()` - Context management for plant-id and workspace-id headers
- Default values for backward compatibility
- Common imports and utilities

### 📁 `system.py`
**System monitoring and status operations**
- `GET /` - Root endpoint (API status)
- `POST /connection/check` - OPC-UA server connection status
- `GET /health` - Basic health check
- `GET /health/detailed` - Detailed health with system info
- `GET /metrics` - Metrics in JSON format
- `GET /metrics/prometheus` - Metrics in Prometheus format

### 📁 `nodes.py`
**OPC-UA node operations**
- `POST /node/value` - Get current node value
- `POST /node/subscribe` - Subscribe to node data changes
- `POST /node/unsubscribe` - Unsubscribe from node
- `GET /node/subscribe/active` - Get active subscriptions
- `GET /node/subscribe/count` - Get subscription counts (memory vs database)

### 📁 `polling.py`
**Polling task management**
- `POST /node/poll/start` - Start polling a node
- `POST /node/poll/stop` - Stop polling a node
- `GET /node/poll/active` - Get active polling tasks

### 📁 `data.py`
**Data retrieval and statistics**
- `POST /data/latest` - Get latest data for a node
- `POST /data/history` - Get historical data with time range
- `POST /data/statistics` - Get data statistics (min, max, avg, etc.)
- `GET /data/recent/{hours}` - Get recent data for all active nodes

## Multi-Database Context Support

All endpoints support optional plant and workspace context via HTTP headers:

```bash
# Optional headers for multi-plant support
-H "plant-id: 1"        # Defaults to "1"
-H "workspace-id: 1"    # Defaults to 1
```

## Usage Examples

### Start Polling (Simple)
```bash
curl -X POST "http://localhost:8007/api/node/poll/start" \
  -H "Content-Type: application/json" \
  -d '{"node_id": "ns=3;i=1006", "interval_seconds": 30}'
```

### Start Polling (Multi-Plant)
```bash
curl -X POST "http://localhost:8007/api/node/poll/start" \
  -H "Content-Type: application/json" \
  -H "plant-id: 2" \
  -H "workspace-id: 5" \
  -d '{"node_id": "ns=3;i=1006", "interval_seconds": 30}'
```

### Get Latest Data
```bash
curl -X POST "http://localhost:8007/api/data/latest" \
  -H "Content-Type: application/json" \
  -H "plant-id: 1" \
  -H "workspace-id: 1" \
  -d '{"node_id": "ns=3;i=1006"}'
```

### Health Check
```bash
curl "http://localhost:8007/api/health"
```

### Get Metrics (Prometheus)
```bash
curl "http://localhost:8007/api/metrics/prometheus"
```

## FastAPI Documentation

The organized routers provide better API documentation with tagged sections:

- **Swagger UI**: `http://localhost:8007/docs`
- **ReDoc**: `http://localhost:8007/redoc`

Each router is tagged for better organization in the documentation:
- `system` - System monitoring and status
- `nodes` - OPC-UA node operations  
- `polling` - Polling task management
- `data` - Data retrieval and statistics

## Migration from Single Endpoints File

The original `api/endpoints.py` has been split into multiple focused routers while maintaining:

✅ **Full backward compatibility** - All existing API calls work unchanged  
✅ **Same URL structure** - No endpoint URLs have changed  
✅ **Enhanced organization** - Logical separation by functionality  
✅ **Better documentation** - Tagged sections in Swagger/ReDoc  
✅ **Improved maintainability** - Smaller, focused files  
✅ **Multi-database support** - Ready for plant-specific operations 
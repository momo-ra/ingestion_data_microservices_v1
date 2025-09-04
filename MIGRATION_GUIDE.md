# 🚀 Datasource Integration Migration Guide

This guide explains how to migrate from the old single OPC UA client to the new multi-datasource architecture.

## 📋 Overview

The migration transforms your system from a single OPC UA connection to a flexible multi-datasource architecture that supports:
- **OPC UA servers**
- **Database connections** (PostgreSQL, MySQL, etc.)
- **Modbus connections**
- **Future datasource types**

## 🔄 Migration Steps

### Step 1: Install Dependencies

```bash
# Install the datasources library
pip install datasources-lib==1.0.0

# Update your requirements.txt
echo "datasources-lib==1.0.0" >> requirements.txt
```

### Step 2: Run Database Migration

```bash
# Run the migration script
python migrations/migrate_to_datasources.py
```

This script will:
- Create default datasource types (OPC UA, Database, Modbus)
- Migrate your existing OPC UA configuration to a datasource
- Update existing tags to use the new datasource

### Step 3: Test the Integration

```bash
# Run the integration tests
python tests/test_datasource_integration.py
```

## 🆕 New API Endpoints

### Data Reading/Writing

#### Read Single Node
```http
POST /api/v1/data/read/{data_source_id}
Content-Type: application/json

{
  "node_id": "ns=3;i=1001"
}
```

#### Read Multiple Nodes
```http
POST /api/v1/data/read-multiple/{data_source_id}?node_ids=["ns=3;i=1001","ns=3;i=1002"]
```

#### Write to Node
```http
POST /api/v1/data/write/{data_source_id}?node_id=ns=3;i=1001&value=42.5
```

#### Query Database
```http
POST /api/v1/data/query/{data_source_id}?sql=SELECT * FROM sensors&params={}
```

#### Test Connection
```http
GET /api/v1/data/test-connection/{data_source_id}
```

### Tag Management

#### Create Tag with Datasource
```http
POST /api/v1/tags/
Content-Type: application/json

{
  "name": "ns=3;i=1001",
  "connection_string": "ns=3;i=1001",
  "data_source_id": 1,
  "description": "Temperature sensor",
  "unit_of_measure": "°C"
}
```

#### Read Data with Connection String
```http
POST /api/v1/data/read-with-connection/{data_source_id}?name=ns=3;i=1001&connection_string=ns=3;i=1001
```

### Understanding Connection String

The `connection_string` field specifies the actual identifier to search for in the datasource:

- **For OPC UA**: Usually the same as `name` (e.g., `ns=3;i=1001`)
- **For Database**: SQL query or table/column identifier
- **For Modbus**: Register address (e.g., `40001`)

**Examples**:
```json
// OPC UA - same values
{
  "name": "temperature_sensor_1",
  "connection_string": "ns=3;i=1001"
}

// Database - different values
{
  "name": "temperature_sensor_1", 
  "connection_string": "SELECT value FROM sensors WHERE id = 1"
}

// Modbus - different values
{
  "name": "pump_status",
  "connection_string": "40001"
}
```

#### Get/Create Tag with Datasource
```http
GET /api/v1/tags/node/{name}/datasource/{data_source_id}
```

## 🔧 Configuration

### Environment Variables

Remove old OPC UA settings:
```bash
# Remove these
OPC_UA_URL=...
OPC_UA_CONNECTION_TIMEOUT=...
OPC_UA_MAX_RECONNECT_ATTEMPTS=...
```

Add new datasource settings:
```bash
# Add these (optional)
DATASOURCE_CONNECTION_POOL_SIZE=10
DATASOURCE_CONNECTION_TIMEOUT=30.0
DATASOURCE_MAX_RETRIES=3
```

### Database Configuration

Datasource configurations are now stored in the database:

```sql
-- View datasource types
SELECT * FROM data_source_types;

-- View datasources
SELECT * FROM data_sources;

-- View tags with datasource
SELECT t.name, t.data_source_id, ds.name as datasource_name 
FROM tags t 
JOIN data_sources ds ON t.data_source_id = ds.id;
```

## 📊 Datasource Management

### Create OPC UA Datasource

```python
from services.datasource_services import create_data_source_service

# Create OPC UA datasource
config = {
    "url": "opc.tcp://server:4840",
    "connection_timeout": 30.0,
    "max_retries": 3,
    "test_node_id": None
}

response = await create_data_source_service(
    session=session,
    name="plant_opcua",
    type_id=opcua_type_id,
    plant_id=1,
    description="Plant OPC UA server",
    connection_config=config
)
```

### Create Database Datasource

```python
# Create PostgreSQL datasource
config = {
    "db_type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "database": "plant_data",
    "username": "user",
    "password": "pass"
}

response = await create_data_source_service(
    session=session,
    name="plant_database",
    type_id=database_type_id,
    plant_id=1,
    description="Plant database",
    connection_config=config
)
```

## 🔄 Backward Compatibility

### Legacy Endpoints

The following endpoints still work but are deprecated:

```http
# Legacy tag creation (uses default OPC UA datasource)
GET /api/v1/tags/node/{name}
```

**Warning**: Legacy endpoints will show a deprecation warning and suggest using the new datasource-aware endpoints.

### Migration Strategy

1. **Phase 1**: Run migration script to set up datasources
2. **Phase 2**: Update your frontend to use new endpoints
3. **Phase 3**: Remove legacy endpoints (optional)

## 🧪 Testing

### Manual Testing

1. **Test Connection**:
   ```bash
   curl -X GET "http://localhost:8007/api/v1/data/test-connection/1" \
        -H "Authorization: Bearer YOUR_TOKEN"
   ```

2. **Create Tag**:
   ```bash
   curl -X POST "http://localhost:8007/api/v1/tags/" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer YOUR_TOKEN" \
        -d '{
          "name": "ns=3;i=1001",
          "connection_string": "ns=3;i=1001",
          "data_source_id": 1,
          "description": "Test tag"
        }'
   ```

3. **Read Data with Connection String**:
   ```bash
   curl -X POST "http://localhost:8007/api/v1/data/read-with-connection/1?name=ns=3;i=1001&connection_string=ns=3;i=1001" \
        -H "Authorization: Bearer YOUR_TOKEN"
   ```

3. **Read Data**:
   ```bash
   curl -X POST "http://localhost:8007/api/v1/data/read/1" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer YOUR_TOKEN" \
        -d '{"node_id": "ns=3;i=1001"}'
   ```

### Automated Testing

```bash
# Run integration tests
python tests/test_datasource_integration.py

# Run migration verification
python migrations/migrate_to_datasources.py
```

## 🚨 Troubleshooting

### Common Issues

1. **"datasources-lib not available"**
   ```bash
   pip install datasources-lib==1.0.0
   ```

2. **"No OPC UA datasource found"**
   - Run the migration script
   - Check that datasource types were created
   - Verify datasource configuration

3. **"Connection failed"**
   - Check datasource configuration
   - Verify network connectivity
   - Test with connection test endpoint

4. **"Tag creation failed"**
   - Ensure datasource is active
   - Check node exists in datasource
   - Verify permissions
   - Check that connection_string is provided and valid

5. **"Connection string column does not exist"**
   - Run the migration script to update database schema
   - Check that the tags table has the connection_string column
   - Verify database migrations were applied correctly

### Debug Mode

Enable debug logging:

```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## 📈 Performance Considerations

### Connection Pooling

The new system uses connection pooling for better performance:

- **Pool Size**: Configurable via environment variables
- **Connection Reuse**: Connections are reused across requests
- **Automatic Cleanup**: Idle connections are automatically closed

### Caching

- **Configuration Cache**: Datasource configs are cached
- **Connection Cache**: Active connections are cached
- **Clear Cache**: Use `connection_manager.clear_cache()` if needed

## 🔮 Future Enhancements

### Planned Features

1. **More Datasource Types**:
   - MQTT
   - HTTP/REST APIs
   - Custom protocols

2. **Advanced Features**:
   - Load balancing
   - Failover
   - Data transformation
   - Real-time streaming

3. **Monitoring**:
   - Connection health monitoring
   - Performance metrics
   - Alerting

## 📞 Support

If you encounter issues:

1. Check the logs for error messages
2. Run the test scripts to verify functionality
3. Review the migration guide
4. Check datasource configuration

## ✅ Migration Checklist

- [ ] Install datasources-lib
- [ ] Run migration script
- [ ] Test connection manager
- [ ] Update API calls to use new endpoints
- [ ] Test tag creation with datasource
- [ ] Verify data reading/writing
- [ ] Update frontend (if applicable)
- [ ] Remove legacy code (optional)

---

**Congratulations!** 🎉 You've successfully migrated to the new multi-datasource architecture! 
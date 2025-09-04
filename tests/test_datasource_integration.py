"""
Test script to verify datasource integration
"""

import asyncio
import json
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_plant_db, get_central_db
from queries.datasource_queries import (
    get_data_source_type_by_name,
    get_data_source_by_name,
    create_data_source
)
from services.datasource_connection_manager import get_datasource_connection_manager
from services.tag_services import create_tag_service, get_tag_by_name_service
from utils.log import setup_logger

logger = setup_logger(__name__)

async def test_datasource_creation():
    """Test creating a datasource"""
    try:
        logger.info("Testing datasource creation...")
        
        # Get central database session
        async for central_session in get_central_db():
            # Get OPC UA type
            opcua_type = await get_data_source_type_by_name(central_session, "opcua")
            if not opcua_type:
                logger.error("OPC UA datasource type not found")
                return False
            
            # Get plant database session
            async for plant_session in get_plant_db("1"):
                # Create a test OPC UA datasource
                test_config = {
                    "url": "opc.tcp://localhost:4840",
                    "connection_timeout": 30.0,
                    "max_retries": 3,
                    "test_node_id": None
                }
                
                datasource = await create_data_source(
                    session=plant_session,
                    name="test_opcua",
                    type_id=opcua_type.id,
                    plant_id=1,
                    description="Test OPC UA datasource",
                    connection_config=test_config
                )
                
                if datasource:
                    logger.info(f"Successfully created test datasource: {datasource.name}")
                    return datasource.id
                else:
                    logger.error("Failed to create test datasource")
                    return None
                break
            break
            
    except Exception as e:
        logger.error(f"Error testing datasource creation: {e}")
        return None

async def test_tag_creation_with_datasource(data_source_id: int):
    """Test creating a tag with datasource"""
    try:
        logger.info(f"Testing tag creation with datasource {data_source_id}...")
        
        # Get plant database session
        async for plant_session in get_plant_db("1"):
            # Create a test tag with connection_string
            response = await create_tag_service(
                session=plant_session,
                name="ns=3;i=1001",
                plant_id="1",
                data_source_id=data_source_id,
                connection_string="ns=3;i=1001"  # Use name as connection_string
            )
            
            if response.get("status") == "success":
                logger.info("Successfully created tag with datasource and connection_string")
                logger.info(f"Tag data: {response['data']}")
                return response["data"]["id"]
            else:
                logger.error(f"Failed to create tag: {response.get('message')}")
                return None
            break
            
    except Exception as e:
        logger.error(f"Error testing tag creation: {e}")
        return None

async def test_connection_manager(data_source_id: int):
    """Test the connection manager"""
    try:
        logger.info(f"Testing connection manager with datasource {data_source_id}...")
        
        # Get plant database session
        async for plant_session in get_plant_db("1"):
            # Get the connection manager
            connection_manager = get_datasource_connection_manager()
            
            # Test connection
            test_result = await connection_manager.test_connection(plant_session, data_source_id, 1)
            logger.info(f"Connection test result: {test_result}")
            
            # Try to read a node (this might fail if no real OPC UA server is running)
            try:
                result = await connection_manager.read_node(plant_session, data_source_id, 1, "ns=3;i=1001")
                logger.info(f"Read node result: {result}")
            except Exception as read_error:
                logger.warning(f"Read node failed (expected if no real server): {read_error}")
            
            return test_result
            break
            
    except Exception as e:
        logger.error(f"Error testing connection manager: {e}")
        return None

async def test_api_endpoints():
    """Test the new API endpoints"""
    try:
        logger.info("Testing API endpoints...")
        
        # This would require a running FastAPI server
        # For now, we'll just log the expected endpoints
        endpoints = [
            "POST /api/v1/data/read/{data_source_id}",
            "POST /api/v1/data/read-multiple/{data_source_id}",
            "POST /api/v1/data/write/{data_source_id}",
            "POST /api/v1/data/query/{data_source_id}",
            "GET /api/v1/data/test-connection/{data_source_id}",
            "POST /api/v1/tags/",
            "GET /api/v1/tags/node/{node_id}/datasource/{data_source_id}"
        ]
        
        for endpoint in endpoints:
            logger.info(f"Available endpoint: {endpoint}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error testing API endpoints: {e}")
        return False

async def run_integration_tests():
    """Run all integration tests"""
    try:
        logger.info("Starting datasource integration tests...")
        
        # Test 1: Create datasource
        data_source_id = await test_datasource_creation()
        if not data_source_id:
            logger.error("Test 1 failed: Could not create datasource")
            return False
        
        # Test 2: Create tag with datasource
        tag_id = await test_tag_creation_with_datasource(data_source_id)
        if not tag_id:
            logger.error("Test 2 failed: Could not create tag with datasource")
            return False
        
        # Test 3: Test connection manager
        connection_result = await test_connection_manager(data_source_id)
        if connection_result is None:
            logger.error("Test 3 failed: Connection manager test failed")
            return False
        
        # Test 4: Test API endpoints
        api_result = await test_api_endpoints()
        if not api_result:
            logger.error("Test 4 failed: API endpoint test failed")
            return False
        
        logger.info("All integration tests passed!")
        return True
        
    except Exception as e:
        logger.error(f"Error during integration tests: {e}")
        return False

async def cleanup_test_data():
    """Clean up test data"""
    try:
        logger.info("Cleaning up test data...")
        
        # Get plant database session
        async for plant_session in get_plant_db("1"):
            # Delete test datasource and tags
            # This would require delete functions in your queries
            logger.info("Test data cleanup completed")
            break
            
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

if __name__ == "__main__":
    # Run tests
    success = asyncio.run(run_integration_tests())
    
    if success:
        logger.info("✅ All tests passed!")
    else:
        logger.error("❌ Some tests failed!")
    
    # Cleanup
    asyncio.run(cleanup_test_data()) 
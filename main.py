import asyncio
import uvicorn
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from utils.response import fail_response

# Import organized routers
from routers import data_routers, nodes_routers, polling_routers, system_routers, tag_routers, datasource_routers
from routers.common_routers import DEFAULT_PLANT_ID, DEFAULT_WORKSPACE_ID

# Import middleware
from middleware import authenticate_user, RequirePermission, Permissions

from services.datasource_connection_manager import get_datasource_connection_manager
from services.polling_services import get_polling_service
from services.subscription_services import get_subscription_service
from services.monitoring_services import MonitoringService
from database import init_db
from utils.log import setup_logger
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('./../.env', override=True)  

# Setup logger
logger = setup_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title="OPC-UA Data Ingestion Service",
    description="Multi-database OPC-UA data ingestion microservice with plant-specific data management",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Global exception handler for HTTPExceptions
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Global exception handler to ensure standardized response format"""
    # Check if the detail is already a standardized response
    if isinstance(exc.detail, dict) and "status" in exc.detail:
        # Already in standardized format, return as is
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.detail
        )
    else:
        # Convert to standardized format
        return JSONResponse(
            status_code=exc.status_code,
            content=fail_response(
                message=str(exc.detail) if exc.detail else "An error occurred",
                data={"error_type": "http_exception", "status_code": exc.status_code}
            )
        )

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Include organized routers with authentication where needed
# System routes (health, metrics) - no authentication required
app.include_router(system_routers.router, prefix="/api/v1")  

# Node operations - require authentication
app.include_router(nodes_routers.router, prefix="/api/v1", dependencies=[Depends(authenticate_user)])   

# Polling operations - require authentication
app.include_router(polling_routers.router, prefix="/api/v1", dependencies=[Depends(authenticate_user)]) 

# Data operations - require authentication
app.include_router(data_routers.router, prefix="/api/v1", dependencies=[Depends(authenticate_user)])    

# Tag operations - require authentication
app.include_router(tag_routers.router, prefix="/api/v1", dependencies=[Depends(authenticate_user)])

# DataSource operations - require authentication
app.include_router(datasource_routers.router, prefix="/api/v1", dependencies=[Depends(authenticate_user)])

async def initialize_services():
    """Initialize all services"""
    try:
        # Initialize database
        logger.info("Initializing database...")
        await init_db()
        
        # Initialize datasource connection manager
        logger.info("Initializing datasource connection manager...")
        connection_manager = get_datasource_connection_manager()
        await connection_manager.start()
        
        # Initialize polling service
        logger.info("Initializing polling service...")
        polling_service = get_polling_service()
        await polling_service.initialize()
        
        # Initialize subscription service
        logger.info("Initializing subscription service...")
        subscription_service = get_subscription_service()
        await subscription_service.initialize()
        
        # Initialize monitoring service
        logger.info("Initializing monitoring service...")
        monitoring_service = MonitoringService.get_instance()
        await monitoring_service.start_monitoring()
        
        logger.info("All services initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing services: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def cleanup_services():
    """Clean up all services"""
    try:
        # Get service instances
        connection_manager = get_datasource_connection_manager()
        subscription_service = get_subscription_service()
        monitoring_service = MonitoringService.get_instance()
        
        # Stop monitoring
        logger.info("Stopping monitoring service...")
        await monitoring_service.stop_monitoring()
        
        # Clean up subscriptions
        logger.info("Cleaning up subscriptions...")
        await subscription_service.cleanup()
        
        # Stop datasource connection manager
        logger.info("Stopping datasource connection manager...")
        await connection_manager.stop()
        
        logger.info("All services cleaned up successfully")
    except Exception as e:
        logger.error(f"Error cleaning up services: {e}")
        import traceback
        logger.error(traceback.format_exc())

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    try:
        # Run the initialization
        loop = asyncio.get_event_loop()
        await initialize_services()
        
        logger.info("Startup completed successfully")
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        import traceback
        logger.error(traceback.format_exc())

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown"""
    try:
        # Clean up
        await cleanup_services()
        
        logger.info("Shutdown completed successfully")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

if __name__ == "__main__":
    # Get port from environment variable or use default
    port = int(os.getenv("PORT", 8007))
    
    # Run the application
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)

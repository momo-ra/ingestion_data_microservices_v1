import asyncio
import uvicorn
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from api.endpoints import router as api_router
from services.opc_ua_services import get_opc_ua_client
from services.polling_services import get_polling_service
from services.subscription_services import get_subscription_service
from services.monitoring_services import MonitoringService
from database import init_db
from utils.log import setup_logger
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env', override=True)  

# Setup logger
logger = setup_logger(__name__)

# Create FastAPI app
app = FastAPI(title="OPC-UA Data Ingestion Service")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Include API router
app.include_router(api_router, prefix="/api")

async def initialize_services():
    """Initialize all services"""
    try:
        # Initialize database
        logger.info("Initializing database...")
        await init_db()
        
        # Initialize OPC UA client
        logger.info("Initializing OPC UA client...")
        opc_client = get_opc_ua_client()
        await opc_client.connect()
        
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
        opc_client = get_opc_ua_client()
        subscription_service = get_subscription_service()
        monitoring_service = MonitoringService.get_instance()
        
        # Stop monitoring
        logger.info("Stopping monitoring service...")
        await monitoring_service.stop_monitoring()
        
        # Clean up subscriptions
        logger.info("Cleaning up subscriptions...")
        await subscription_service.cleanup()
        
        # Disconnect OPC UA client
        logger.info("Disconnecting OPC UA client...")
        await opc_client.disconnect()
        
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

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "OPC-UA Data Ingestion Service is running"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    monitoring_service = MonitoringService.get_instance()
    health_status = monitoring_service.get_health_status()
    return health_status

if __name__ == "__main__":
    # Get port from environment variable or use default
    port = int(os.getenv("PORT", 8007))
    
    # Run the application
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import PlainTextResponse
from services.opc_ua_services import get_opc_ua_client
from services.monitoring_services import MonitoringService
from utils.metrics import get_metrics
from utils.log import setup_logger
from utils.response import success_response, fail_response
from database import get_central_db
from middleware import authenticate_user, RequirePermission, Permissions, get_user_id
from sqlalchemy import text
import time
import psutil
import platform

router = APIRouter(tags=["system"])
logger = setup_logger(__name__)

@router.get("/")
async def root():
    """Root endpoint - API status"""
    return success_response(
        data={"service": "OPC-UA API"},
        message="OPC-UA API is running"
    )

@router.get("/plants")
async def get_plants():
    """Get all plants from the registry"""
    try:
        async for session in get_central_db():
            query = text("SELECT id, name, description, connection_key, database_key, is_active FROM plants_registry")
            result = await session.execute(query)
            plants = result.fetchall()
            
            plant_list = []
            for plant in plants:
                plant_list.append({
                    "id": plant.id,
                    "name": plant.name,
                    "description": plant.description,
                    "connection_key": plant.connection_key,
                    "database_key": plant.database_key,
                    "is_active": plant.is_active
                })
            
            return success_response(
                data={
                    "plants": plant_list,
                    "count": len(plant_list)
                },
                message=f"Retrieved {len(plant_list)} plants from registry"
            )
            break
    except Exception as e:
        logger.error(f"Error getting plants: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/connection/check")
async def check_connection():
    """Check the connection to the OPC-UA server"""
    try:
        client = get_opc_ua_client()
        if client.connected:
            return success_response(
                data={"connected": True},
                message="Connected to OPC-UA server"
            )
        else:
            # Try to connect
            success = await client.connect()
            if success:
                return success_response(
                    data={"connected": True},
                    message="Successfully connected to OPC-UA server"
                )
            else:
                return fail_response(
                    message="Failed to connect to OPC-UA server",
                    data={"connected": False}
                )
    except Exception as e:
        logger.error(f"Error checking connection: {e}")
        return fail_response(
            message=f"Error checking connection: {str(e)}",
            data={"connected": False, "error": str(e)}
        )

@router.get("/health")
async def get_health():
    """Get the health status of the system"""
    try:
        monitoring_service = MonitoringService.get_instance()
        health_status = monitoring_service.get_health_status()
        return success_response(
            data=health_status,
            message="Retrieved system health status"
        )
    except Exception as e:
        logger.error(f"Error getting health status: {e}")
        return fail_response(
            message=f"Error getting health status: {str(e)}",
            data={"status": "error"}
        )

@router.get("/health/detailed")
async def get_detailed_health():
    """Get detailed health status with metrics for all components"""
    try:
        monitoring_service = MonitoringService.get_instance()
        health_status = monitoring_service.get_health_status()
        
        # Add additional system information
        system_info = {
            "cpu_percent": psutil.cpu_percent(),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage('/').percent,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "process_uptime": time.time() - psutil.Process().create_time()
        }
        
        health_status["system_info"] = system_info
        return success_response(
            data=health_status,
            message="Retrieved detailed system health status"
        )
    except Exception as e:
        logger.error(f"Error getting detailed health status: {e}")
        return fail_response(
            message=f"Error getting detailed health status: {str(e)}",
            data={"status": "error"}
        )

@router.get("/metrics")
async def metrics():
    """Get all metrics in JSON format"""
    return success_response(
        data=get_metrics(),
        message="Retrieved system metrics"
    )

@router.get("/metrics/prometheus")
async def prometheus_metrics():
    """Get metrics in Prometheus format"""
    metrics_data = get_metrics()
    
    # Convert to Prometheus format
    lines = []
    for metric in metrics_data["metrics"]:
        # Add metric metadata
        lines.append(f"# HELP {metric['name']} {metric['description']}")
        lines.append(f"# TYPE {metric['name']} {metric['type']}")
        
        # Add metric values based on type
        if metric['type'] == 'counter' or metric['type'] == 'gauge':
            for value_entry in metric.get('values', []):
                if 'labels' in value_entry:
                    label_str = ','.join([f'{k}="{v}"' for k, v in value_entry['labels'].items()])
                    lines.append(f"{metric['name']}{{{label_str}}} {value_entry['value']}")
                else:
                    lines.append(f"{metric['name']} {value_entry['value']}")
                    
        elif metric['type'] == 'histogram':
            for value_entry in metric.get('values', []):
                # Extract labels
                if 'labels' in value_entry:
                    label_str = ','.join([f'{k}="{v}"' for k, v in value_entry['labels'].items()])
                    label_part = f"{{{label_str}}}"
                else:
                    label_part = ""
                
                # Add sum and count
                lines.append(f"{metric['name']}_sum{label_part} {value_entry['sum']}")
                lines.append(f"{metric['name']}_count{label_part} {value_entry['count']}")
                
                # Add buckets
                for bucket in value_entry['buckets']:
                    # Add le label to existing labels
                    if 'labels' in value_entry:
                        bucket_label_str = ','.join([f'{k}="{v}"' for k, v in value_entry['labels'].items()])
                        bucket_label_str += f',le="{bucket["le"]}"'
                    else:
                        bucket_label_str = f'le="{bucket["le"]}"'
                    
                    lines.append(f"{metric['name']}_bucket{{{bucket_label_str}}} {bucket['count']}")
    
    return PlainTextResponse("\n".join(lines))

# Protected endpoints that require authentication and permissions

@router.get("/admin/system-info")
async def get_system_info(
    auth_data: dict = Depends(authenticate_user),
    permission_check: dict = Depends(RequirePermission(Permissions.ADMIN_ACCESS))
):
    """Get detailed system information - requires admin access"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"Admin user {user_id} accessing system info")
        
        system_info = {
            "user_id": user_id,
            "cpu_percent": psutil.cpu_percent(),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage('/').percent,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "process_uptime": time.time() - psutil.Process().create_time(),
            "auth_data": auth_data
        }
        
        return success_response(
            data=system_info,
            message="Retrieved admin system information"
        )
    except Exception as e:
        logger.error(f"Error getting admin system info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/user/profile")
async def get_user_profile(auth_data: dict = Depends(authenticate_user)):
    """Get current user profile information"""
    try:
        user_id = get_user_id(auth_data)
        logger.info(f"User {user_id} accessing profile")
        
        # Get user information from database
        async for session in get_central_db():
            query = text("""
                SELECT id, username, email, is_active, created_at 
                FROM users 
                WHERE id = :user_id
            """)
            result = await session.execute(query, {"user_id": user_id})
            user_info = result.fetchone()
            
            if not user_info:
                raise HTTPException(status_code=404, detail="User not found")
            
            profile_data = {
                "id": user_info.id,
                "username": user_info.username,
                "email": user_info.email,
                "is_active": user_info.is_active,
                "created_at": user_info.created_at.isoformat() if user_info.created_at else None,
                "roles": auth_data.get("roles", [])
            }
            
            return success_response(
                data=profile_data,
                message="Retrieved user profile"
            )
            break
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user profile: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 
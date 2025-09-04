from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, TypeVar, Generic

T = TypeVar('T')

# General Class for Response
class ResponseModel(BaseModel, Generic[T]):
    """Base model for standardized API responses"""
    status: str = Field(..., description="Status of Response(success/fail)")
    data: Optional[T] = Field(None, description="Data should be return")
    message: Optional[str] = Field(None, description="Error message when fail happened")

# Standardized Request Models
class NodeRequest(BaseModel):
    """Standard request model for node operations"""
    node_id: str = Field(..., description="OPC-UA node identifier")
    
class PollingRequest(BaseModel):
    """Standard request model for polling operations"""
    node_id: str = Field(..., description="OPC-UA node identifier")
    interval_seconds: int = Field(60, description="Polling interval in seconds")

class TimeRangeRequest(BaseModel):
    """Standard request model for time-range operations"""
    node_id: str = Field(..., description="OPC-UA node identifier")
    start_time: Optional[datetime] = Field(None, description="Start time for data range")
    end_time: Optional[datetime] = Field(None, description="End time for data range")
    limit: Optional[int] = Field(100, description="Maximum number of records to return")

# Tag Request Models
class TagCreateRequest(BaseModel):
    """Request model for creating a new tag"""
    name: str = Field(..., description="Tag name in the database")
    connection_string: str = Field(..., description="Connection string (node ID) to search in datasource")
    data_source_id: int = Field(..., description="Data source ID for the tag")
    description: Optional[str] = Field(None, description="Tag description")
    unit_of_measure: Optional[str] = Field("unknown", description="Unit of measure for the tag")

class TagUpdateRequest(BaseModel):
    """Request model for updating a tag"""
    name: Optional[str] = Field(None, description="Tag name")
    description: Optional[str] = Field(None, description="Tag description")
    unit_of_measure: Optional[str] = Field(None, description="Unit of measure for the tag")
    is_active: Optional[bool] = Field(None, description="Whether the tag is active")

# DataSource Type Request Models
class DataSourceTypeCreateRequest(BaseModel):
    """Request model for creating a new data source type"""
    name: str = Field(..., description="Data source type name")
    description: Optional[str] = Field(None, description="Data source type description")

class DataSourceTypeUpdateRequest(BaseModel):
    """Request model for updating a data source type"""
    name: Optional[str] = Field(None, description="Data source type name")
    description: Optional[str] = Field(None, description="Data source type description")
    is_active: Optional[bool] = Field(None, description="Whether the data source type is active")

# DataSource Request Models
class DataSourceCreateRequest(BaseModel):
    """Request model for creating a new data source"""
    name: str = Field(..., description="Data source name")
    type_id: int = Field(..., description="Data source type ID")
    description: Optional[str] = Field(None, description="Data source description")
    connection_config: Optional[dict] = Field(None, description="Connection configuration")

class DataSourceUpdateRequest(BaseModel):
    """Request model for updating a data source"""
    name: Optional[str] = Field(None, description="Data source name")
    type_id: Optional[int] = Field(None, description="Data source type ID")
    description: Optional[str] = Field(None, description="Data source description")
    connection_config: Optional[dict] = Field(None, description="Connection configuration")
    is_active: Optional[bool] = Field(None, description="Whether the data source is active")

class DataSourceTestConnectionRequest(BaseModel):
    """Request model for testing data source connection"""
    type_id: int = Field(..., description="Data source type ID")
    connection_config: dict = Field(..., description="Connection configuration to test")

# Data Models
class TagSchema(BaseModel):
    id: Optional[int] = None
    name: str = Field(..., example="Tag Name")
    description: Optional[str] = Field(None, example="Tag Description")
    unit_of_measure: Optional[str] = Field(None, example="Tag Unit")
    
class TimeSeriesSchema(BaseModel):
    id: Optional[int] = None
    tag_id: int = Field(..., example="Tag ID")
    timestamp: datetime = Field(..., example="2023-03-10T12:00:00")
    value: str = Field(..., example="Tag Value")

class KafkaMessageSchema(BaseModel):
    tag_name: str = Field(..., example="Tag Name")
    tag_id: int = Field(..., example="Tag ID")
    value: str = Field(..., example="Tag Value")
    unit_of_measure: str = Field(..., example="Tag Unit")
    description: str = Field(..., example="Tag Description")
    timestamp: datetime = Field(..., example="2023-03-10T12:00:00")

# Legacy models for backward compatibility (deprecated)
class PollingResponse(BaseModel):
    """Legacy polling response model - deprecated, use ResponseModel instead"""
    success: bool
    message: str
    node_id: str
    interval_seconds: Optional[int] = None

    
    
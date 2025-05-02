from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

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

    
    
    
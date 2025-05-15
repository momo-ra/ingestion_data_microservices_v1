from typing import Any, Dict, Optional
from schemas.schema import ResponseModel

def success_response(data:Any = None) -> Dict[str, Any]:
    return ResponseModel(status= "success", data = data, message=None)

def error_response( message: str, data:Any = None)->Dict[str, Any]:
    return ResponseModel(status= "fail", data = None, message = message)
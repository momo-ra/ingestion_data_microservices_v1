"""
Error Handling Utilities

This module provides standardized error handling mechanisms including:
- Custom exception classes for different error types
- Error handling decorators for consistent error management
- Utility functions for error reporting
"""

import functools
import logging
import traceback
import sys
from typing import Any, Callable, Dict, Optional, Type, TypeVar, cast, Union

# Setup logging
logger = logging.getLogger(__name__)

# Type variables for decorator typing
F = TypeVar('F', bound=Callable[..., Any])
T = TypeVar('T')
R = TypeVar('R')

class OpcUaError(Exception):
    """Base exception for all OPC UA related errors"""
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        super().__init__(self.message)

class ConnectionError(OpcUaError):
    """Exception raised for OPC UA connection errors"""
    pass

class SubscriptionError(OpcUaError):
    """Exception raised for OPC UA subscription errors"""
    pass

class PollingError(OpcUaError):
    """Exception raised for OPC UA polling errors"""
    pass

class DataProcessingError(OpcUaError):
    """Exception raised for data processing errors"""
    pass

class DatabaseError(OpcUaError):
    """Exception raised for database errors"""
    pass

class ConfigurationError(OpcUaError):
    """Exception raised for configuration errors"""
    pass

def format_exception(exc: Exception) -> Dict[str, Any]:
    """Format an exception into a structured dictionary

    Args:
        exc: The exception to format

    Returns:
        Dict with error details
    """
    error_type = type(exc).__name__
    error_message = str(exc)
    error_traceback = traceback.format_exception(type(exc), exc, exc.__traceback__)
    
    error_info = {
        "type": error_type,
        "message": error_message,
        "traceback": error_traceback,
    }
    
    # Add additional fields from OpcUaError
    if isinstance(exc, OpcUaError):
        error_info["code"] = exc.error_code
        error_info["details"] = exc.details
        
    return error_info

def handle_async_errors(
    error_class: Type[Exception] = OpcUaError,
    default_message: str = "An error occurred",
    raise_error: bool = True,
    log_error: bool = True,
    return_value: Any = None
) -> Callable[[F], F]:
    """Decorator for handling errors in async functions

    Args:
        error_class: The exception class to use for wrapping errors
        default_message: Default error message
        raise_error: Whether to raise the error after logging
        log_error: Whether to log the error
        return_value: Value to return in case of error if not raising

    Returns:
        Decorated function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Log the error
                if log_error:
                    error_details = format_exception(e)
                    logger.error(
                        f"Error in {func.__name__}: {str(e)}", 
                        extra={"error_details": error_details}
                    )
                
                # Determine if we need to wrap the exception
                if not isinstance(e, error_class):
                    wrapped_error = error_class(
                        message=str(e) or default_message,
                        details={"original_error": type(e).__name__}
                    )
                else:
                    wrapped_error = e
                
                # Raise or return
                if raise_error:
                    raise wrapped_error from e
                return return_value
                
        return cast(F, wrapper)
    return decorator

def handle_errors(
    error_class: Type[Exception] = OpcUaError,
    default_message: str = "An error occurred",
    raise_error: bool = True,
    log_error: bool = True,
    return_value: Any = None
) -> Callable[[F], F]:
    """Decorator for handling errors in sync functions

    Args:
        error_class: The exception class to use for wrapping errors
        default_message: Default error message
        raise_error: Whether to raise the error after logging
        log_error: Whether to log the error
        return_value: Value to return in case of error if not raising

    Returns:
        Decorated function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Log the error
                if log_error:
                    error_details = format_exception(e)
                    logger.error(
                        f"Error in {func.__name__}: {str(e)}", 
                        extra={"error_details": error_details}
                    )
                
                # Determine if we need to wrap the exception
                if not isinstance(e, error_class):
                    wrapped_error = error_class(
                        message=str(e) or default_message,
                        details={"original_error": type(e).__name__}
                    )
                else:
                    wrapped_error = e
                
                # Raise or return
                if raise_error:
                    raise wrapped_error from e
                return return_value
                
        return cast(F, wrapper)
    return decorator 
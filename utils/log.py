"""
Logging Utilities

This module provides enhanced logging capabilities with:
- Structured logging with consistent fields
- Correlation IDs for tracking operations across components
- Configurable outputs (console, file, etc.)
- Log level management
"""

import logging
import logging.handlers
import os
import sys
import uuid
import json
from typing import Optional, Dict, Any, Union
from datetime import datetime
import traceback
import threading
from functools import wraps
from contextvars import ContextVar

# Import settings if available, otherwise use defaults
try:
    from config.settings import settings
except ImportError:
    # Default settings for standalone usage
    class Settings:
        class LoggingSettings:
            level = "INFO"
            format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            file_path = None
            log_to_console = True
            log_to_file = False
        logging = LoggingSettings()
    settings = Settings()

# Context variable to store the correlation ID
correlation_id: ContextVar[str] = ContextVar('correlation_id', default='')

def get_correlation_id() -> str:
    """Get the current correlation ID or generate a new one"""
    current_id = correlation_id.get()
    if not current_id:
        new_id = str(uuid.uuid4())
        correlation_id.set(new_id)
        return new_id
    return current_id

def set_correlation_id(new_id: str) -> None:
    """Set the correlation ID for the current context"""
    correlation_id.set(new_id)

class StructuredLogRecord(logging.LogRecord):
    """Extended LogRecord with structured data support"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.correlation_id = get_correlation_id()
        self.structured_data = {}

class StructuredLogger(logging.Logger):
    """Logger that creates StructuredLogRecord instances"""
    def makeRecord(self, name, level, fn, lno, msg, args, exc_info, func=None, extra=None, sinfo=None):
        record = StructuredLogRecord(name, level, fn, lno, msg, args, exc_info, func, sinfo)
        if extra:
            for key, value in extra.items():
                if key == 'structured_data' and isinstance(value, dict):
                    record.structured_data.update(value)
                else:
                    setattr(record, key, value)
        return record
    
    # Custom logging methods for backward compatibility
    def success(self, message, *args, **kwargs):
        """Success log with checkmark emoji for backward compatibility"""
        self.info(f"✅ {message}", *args, **kwargs)
    
    def danger(self, message, *args, **kwargs):
        """Danger log with X emoji for backward compatibility"""
        self.error(f"❌ {message}", *args, **kwargs)
    
    def warn_custom(self, message, *args, **kwargs):
        """Warning log with warning emoji for backward compatibility"""
        self.warning(f"⚠️ {message}", *args, **kwargs)

class JsonFormatter(logging.Formatter):
    """Formatter that outputs JSON strings"""
    def __init__(self, fmt=None, datefmt=None, style='%', include_stack_info=False):
        super().__init__(fmt, datefmt, style)
        self.include_stack_info = include_stack_info

    def format(self, record):
        log_data = {
            'timestamp': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'name': record.name,
            'message': record.getMessage(),
            'correlation_id': getattr(record, 'correlation_id', ''),
            'thread': record.threadName,
            'file': record.pathname,
            'line': record.lineno,
            'function': record.funcName,
        }
        
        # Add structured data
        if hasattr(record, 'structured_data') and record.structured_data:
            log_data['data'] = record.structured_data
            
        # Add any extra attributes
        for key, value in record.__dict__.items():
            if key not in ['args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
                           'funcName', 'id', 'levelname', 'levelno', 'lineno', 'module',
                           'msecs', 'message', 'msg', 'name', 'pathname', 'process',
                           'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName',
                           'correlation_id', 'structured_data']:
                log_data[key] = value
                
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': traceback.format_exception(*record.exc_info) if self.include_stack_info else None
            }
            
        return json.dumps(log_data)

def setup_logger(
    name: str, 
    level: Optional[str] = None,
    log_file: Optional[str] = None,
    log_to_console: Optional[bool] = None,
    log_to_file: Optional[bool] = None,
    json_output: bool = False,
    include_stack_info: bool = False
) -> logging.Logger:
    """Configure and return a logger

    Args:
        name: The logger name (usually __name__)
        level: The logging level (DEBUG, INFO, etc.)
        log_file: The log file path
        log_to_console: Whether to log to console
        log_to_file: Whether to log to file
        json_output: Whether to format logs as JSON
        include_stack_info: Whether to include stack traces in logs

    Returns:
        Configured logger instance
    """
    # Use settings if parameters not provided
    level = level or settings.logging.level
    log_file = log_file or settings.logging.file_path
    log_to_console = log_to_console if log_to_console is not None else settings.logging.log_to_console
    log_to_file = log_to_file if log_to_file is not None else settings.logging.log_to_file
    
    # Register the StructuredLogger class
    logging.setLoggerClass(StructuredLogger)
    
    # Get the logger
    logger = logging.getLogger(name)
    
    # Set the logging level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    logger.setLevel(numeric_level)
    
    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        
    # Create formatter
    if json_output:
        formatter = JsonFormatter(include_stack_info=include_stack_info)
    else:
        formatter = logging.Formatter(
            settings.logging.format,
            '%Y-%m-%d %H:%M:%S'
        )
    
    # Add console handler if requested
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # Add file handler if requested
    if log_to_file and log_file:
        # Ensure directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Create rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10485760,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def with_correlation_id(func):
    """Decorator to ensure a correlation ID is set for the function call"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Set correlation ID if not already set
        if not correlation_id.get():
            set_correlation_id(str(uuid.uuid4()))
        return func(*args, **kwargs)
    return wrapper

def with_async_correlation_id(func):
    """Decorator to ensure a correlation ID is set for async function calls"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Set correlation ID if not already set
        if not correlation_id.get():
            set_correlation_id(str(uuid.uuid4()))
        return await func(*args, **kwargs)
    return wrapper

# Create a default logger for importing modules
default_logger = setup_logger('default')


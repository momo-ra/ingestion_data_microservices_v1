"""
Singleton Base Class

This module provides a base class for implementing the Singleton pattern.
"""

class Singleton:
    """
    A base class that implements the singleton pattern.
    
    Classes that inherit from this class will only have one instance.
    """
    _instances = {}
    
    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__new__(cls)
        return cls._instances[cls]
        
    @classmethod
    def get_instance(cls, *args, **kwargs):
        """Get or create the singleton instance of this class"""
        if cls not in cls._instances:
            cls._instances[cls] = cls(*args, **kwargs)
        return cls._instances[cls]
        
    @classmethod
    def reset_instance(cls):
        """Reset the singleton instance (primarily for testing)"""
        if cls in cls._instances:
            del cls._instances[cls] 
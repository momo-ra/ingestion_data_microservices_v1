"""
Task Manager

Centralized management of asyncio tasks with monitoring and lifecycle tracking.
"""

import asyncio
import time
from typing import Dict, List, Any, Coroutine, Optional, Callable
from utils.log import setup_logger
from utils.singleton import Singleton

logger = setup_logger(__name__)

class TaskManager(Singleton):
    """
    Manages asyncio tasks with tracking, monitoring, and graceful shutdown.
    """
    
    def __init__(self):
        """Initialize the task manager"""
        # Avoid re-initialization if already initialized
        if hasattr(self, 'initialized') and self.initialized:
            return
            
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.monitoring_task = None
        self.monitoring_running = False
        self.initialized = True
        
    async def start_task(self, name: str, coro: Coroutine, restart_on_failure: bool = False,
                        max_restarts: int = 3, restart_delay: int = 5) -> bool:
        """
        Start a new background task with the given name
        
        Args:
            name: Unique name for the task
            coro: Coroutine to run as a task
            restart_on_failure: Whether to restart the task if it fails
            max_restarts: Maximum number of restarts (only relevant if restart_on_failure is True)
            restart_delay: Delay in seconds before restarting (only relevant if restart_on_failure is True)
            
        Returns:
            bool: Whether the task was started successfully
        """
        if name in self.tasks and not self.tasks[name]['task'].done():
            logger.warning(f"Task {name} is already running")
            return False
            
        async def _wrapped_task():
            restarts = 0
            while True:
                try:
                    start_time = time.time()
                    await coro
                    logger.info(f"Task {name} completed successfully")
                    break
                except asyncio.CancelledError:
                    logger.info(f"Task {name} was cancelled")
                    break
                except Exception as e:
                    logger.error(f"Task {name} failed with error: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    
                    if restart_on_failure and restarts < max_restarts:
                        restarts += 1
                        logger.info(f"Restarting failed task {name} (attempt {restarts}/{max_restarts})...")
                        await asyncio.sleep(restart_delay)
                    else:
                        if restart_on_failure:
                            logger.warning(f"Task {name} reached maximum restarts ({max_restarts})")
                        break
        
        task = asyncio.create_task(_wrapped_task())
        self.tasks[name] = {
            'task': task,
            'start_time': time.time(),
            'restart_on_failure': restart_on_failure,
            'max_restarts': max_restarts,
            'restart_delay': restart_delay
        }
        logger.info(f"Started task {name}")
        
        # Start monitoring if not already running
        await self.start_monitoring()
        
        return True
        
    async def stop_task(self, name: str) -> bool:
        """
        Stop a running task by name
        
        Args:
            name: The name of the task to stop
            
        Returns:
            bool: Whether the task was stopped successfully
        """
        if name not in self.tasks:
            logger.warning(f"No task found with name {name}")
            return False
            
        task_info = self.tasks[name]
        task = task_info['task']
        
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            logger.info(f"Stopped task {name}")
        
        del self.tasks[name]
        return True
        
    async def stop_all_tasks(self) -> bool:
        """
        Stop all running tasks
        
        Returns:
            bool: Whether all tasks were stopped successfully
        """
        task_names = list(self.tasks.keys())
        
        for name in task_names:
            await self.stop_task(name)
            
        return True
        
    def get_task_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Get status of all managed tasks
        
        Returns:
            Dict: Dictionary with task statuses
        """
        status = {}
        current_time = time.time()
        
        for name, task_info in self.tasks.items():
            task = task_info['task']
            start_time = task_info['start_time']
            
            status[name] = {
                'status': 'running' if not task.done() else 
                         'completed' if not task.cancelled() else 'cancelled',
                'uptime': current_time - start_time,
                'restart_on_failure': task_info['restart_on_failure'],
                'max_restarts': task_info['max_restarts']
            }
            
        return status
        
    async def start_monitoring(self) -> bool:
        """
        Start monitoring tasks
        
        Returns:
            bool: Whether monitoring was started successfully
        """
        if self.monitoring_running:
            return True
            
        self.monitoring_running = True
        self.monitoring_task = asyncio.create_task(self._monitoring_worker())
        logger.debug("Task monitoring started")
        return True
        
    async def stop_monitoring(self) -> bool:
        """
        Stop monitoring tasks
        
        Returns:
            bool: Whether monitoring was stopped successfully
        """
        if not self.monitoring_running:
            return True
            
        self.monitoring_running = False
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        logger.debug("Task monitoring stopped")
        return True
        
    async def _monitoring_worker(self) -> None:
        """Worker function for task monitoring"""
        try:
            while self.monitoring_running:
                # Check for completed tasks and clean up
                for name, task_info in list(self.tasks.items()):
                    task = task_info['task']
                    if task.done():
                        try:
                            # Check if there was an exception
                            exc = task.exception()
                            if exc:
                                logger.warning(f"Task {name} failed with exception: {exc}")
                        except asyncio.InvalidStateError:
                            # Task was cancelled
                            pass
                        except Exception as e:
                            logger.error(f"Error checking task {name} status: {e}")
                            
                # Sleep before next check
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            logger.debug("Task monitoring cancelled")
        except Exception as e:
            logger.error(f"Error in task monitoring: {e}")
            import traceback
            logger.error(traceback.format_exc()) 
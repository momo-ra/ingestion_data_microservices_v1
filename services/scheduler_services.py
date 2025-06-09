from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from utils.log import setup_logger

logger = setup_logger(__name__)

class SchedulerService:
    """Service for managing scheduled tasks"""
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get the singleton instance of SchedulerService"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """Initialize the scheduler service"""
        jobstores = {
            'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
        }
        self.scheduler = AsyncIOScheduler(jobstores=jobstores)
        self.jobs = {}
        
    def start(self):
        """Start the scheduler"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")
        
    def shutdown(self):
        """Shutdown the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler shutdown")
            
    def add_job(self, job_id, func, interval_seconds, args=None, kwargs=None, replace_existing=True):
        """Add a job to the scheduler
        
        Args:
            job_id (str): The job ID
            func (callable): The function to call
            interval_seconds (int): The interval in seconds
            args (list, optional): The positional arguments to pass to the function. Defaults to None.
            kwargs (dict, optional): The keyword arguments to pass to the function. Defaults to None.
            replace_existing (bool, optional): Whether to replace an existing job with the same ID. Defaults to True.
            
        Returns:
            apscheduler.job.Job: The job instance
        """
        if not args:
            args = []
        if not kwargs:
            kwargs = {}
            
        # Start the scheduler if not running
        if not self.scheduler.running:
            self.start()
            
        # Add the job
        job = self.scheduler.add_job(
            func,
            IntervalTrigger(seconds=interval_seconds),
            args=args,
            kwargs=kwargs,
            id=job_id,
            replace_existing=replace_existing
        )
        
        # Store the job
        self.jobs[job_id] = {
            "job": job,
            "interval": interval_seconds,
            "func": func.__name__,
            "args": args,
            "kwargs": kwargs
        }
        
        logger.info(f"Added job: id={job_id}, func={func.__name__}, interval={interval_seconds}s")
        return job
        
    def remove_job(self, job_id):
        """Remove a job from the scheduler
        
        Args:
            job_id (str): The job ID
            
        Returns:
            bool: True if the job was removed, False otherwise
        """
        try:
            if job_id in self.jobs:
                self.scheduler.remove_job(job_id)
                del self.jobs[job_id]
                logger.info(f"Removed job: id={job_id}")
                return True
            else:
                logger.warning(f"Job not found: id={job_id}")
                return False
        except Exception as e:
            logger.error(f"Error removing job: id={job_id}: {e}")
            return False
            
    def get_job(self, job_id):
        """Get a job from the scheduler
        
        Args:
            job_id (str): The job ID
            
        Returns:
            dict: The job information
        """
        return self.jobs.get(job_id)
        
    def get_all_jobs(self):
        """Get all jobs from the scheduler
        
        Returns:
            dict: The jobs
        """
        return self.jobs
        
    def pause_job(self, job_id):
        """Pause a job
        
        Args:
            job_id (str): The job ID
            
        Returns:
            bool: True if the job was paused, False otherwise
        """
        try:
            if job_id in self.jobs:
                self.scheduler.pause_job(job_id)
                logger.info(f"Paused job: id={job_id}")
                return True
            else:
                logger.warning(f"Job not found: id={job_id}")
                return False
        except Exception as e:
            logger.error(f"Error pausing job: id={job_id}: {e}")
            return False
            
    def resume_job(self, job_id):
        """Resume a job
        
        Args:
            job_id (str): The job ID
            
        Returns:
            bool: True if the job was resumed, False otherwise
        """
        try:
            if job_id in self.jobs:
                self.scheduler.resume_job(job_id)
                logger.info(f"Resumed job: id={job_id}")
                return True
            else:
                logger.warning(f"Job not found: id={job_id}")
                return False
        except Exception as e:
            logger.error(f"Error resuming job: id={job_id}: {e}")
            return False

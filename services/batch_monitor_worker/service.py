import asyncio
import logging
import os
import requests
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configuration from environment variables
DATABASE_API_BASE_URL = os.getenv("DATABASE_URL", "http://ingestion-api:8000")
WORKBENCH_API_BASE_URL = os.getenv("WORKBENCH_API_BASE_URL", "http://workbench-api:8080")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))  # seconds
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

class BatchMonitorWorker:
    def __init__(self):
        self.running = True
        self.poll_interval = POLL_INTERVAL
        
    async def start(self):
        """Start the monitoring worker"""
        logger.info("Starting Batch Monitor Worker...")
        logger.info(f"Database API: {DATABASE_API_BASE_URL}")
        logger.info(f"Workbench API: {WORKBENCH_API_BASE_URL}")
        logger.info(f"Poll interval: {self.poll_interval} seconds")
        
        while self.running:
            try:
                await self.poll_and_update_jobs()
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.poll_interval)
    
    async def poll_and_update_jobs(self):
        """Poll database for running jobs and update their status"""
        try:
            # Get all running pipeline runs
            running_jobs = await self.get_running_pipeline_runs()
            
            if not running_jobs:
                logger.debug("No running jobs found")
                return
            
            logger.info(f"Found {len(running_jobs)} running jobs to check")
            
            for job in running_jobs:
                await self.check_and_update_job_status(job)
                
        except Exception as e:
            logger.error(f"Error polling jobs: {e}")
    
    async def get_running_pipeline_runs(self) -> List[Dict[str, Any]]:
        """Get all pipeline runs with RUNNING or SUBMITTED status"""
        try:
            # Get pipeline runs with running status
            url = f"{DATABASE_API_BASE_URL}/pipeline-runs"
            params = {"status": "RUNNING,SUBMITTED"}  # You'll need to implement this filter
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"Failed to get running jobs: {response.status_code} - {response.text}")
                return []
            
            jobs = response.json()
            
            # Filter for jobs that have workbench_job_id
            running_jobs = [
                job for job in jobs 
                if job.get("workbench_job_id") and job.get("status") in ["RUNNING", "SUBMITTED"]
            ]
            
            return running_jobs
            
        except Exception as e:
            logger.error(f"Error getting running pipeline runs: {e}")
            return []
    
    async def check_and_update_job_status(self, job: Dict[str, Any]):
        """Check job status with workbench API and update database"""
        try:
            pipeline_run_id = job.get("id")
            workbench_job_id = job.get("workbench_job_id")
            current_status = job.get("status")
            
            if not workbench_job_id:
                logger.warning(f"Pipeline run {pipeline_run_id} has no workbench_job_id")
                return
            
            logger.debug(f"Checking status for pipeline_run_id: {pipeline_run_id}, workbench_job_id: {workbench_job_id}")
            
            # Get status from workbench API
            workbench_status = await self.get_workbench_job_status(workbench_job_id)
            
            if not workbench_status:
                logger.warning(f"Could not get status for workbench job {workbench_job_id}")
                return
            
            # Map workbench status to our pipeline status
            new_status = self.map_workbench_status(workbench_status.get("status"))
            
            # Update if status changed
            if new_status and new_status != current_status:
                logger.info(f"Status change detected for pipeline_run {pipeline_run_id}: {current_status} -> {new_status}")
                await self.update_pipeline_run_status(pipeline_run_id, new_status, workbench_status)
            
        except Exception as e:
            logger.error(f"Error checking job status for pipeline_run {job.get('id')}: {e}")
    
    async def get_workbench_job_status(self, workbench_job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status from workbench API"""
        try:
            # You'll need to provide the actual workbench API endpoint
            url = f"{WORKBENCH_API_BASE_URL}/jobs/{workbench_job_id}/status"
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 404:
                logger.warning(f"Workbench job {workbench_job_id} not found")
                return None
            elif response.status_code != 200:
                logger.error(f"Failed to get workbench job status: {response.status_code} - {response.text}")
                return None
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Error calling workbench API for job {workbench_job_id}: {e}")
            return None
    
    def map_workbench_status(self, workbench_status: str) -> Optional[str]:
        """Map workbench status to pipeline run status"""
        # You'll need to adjust this mapping based on your workbench API status values
        status_mapping = {
            "SUBMITTED": "SUBMITTED",
            "RUNNING": "RUNNING", 
            "IN_PROGRESS": "RUNNING",
            "PROCESSING": "RUNNING",
            "COMPLETED": "COMPLETED",
            "SUCCESS": "COMPLETED",
            "FINISHED": "COMPLETED",
            "FAILED": "FAILED",
            "ERROR": "FAILED",
            "CANCELLED": "CANCELLED",
            "CANCELED": "CANCELLED"
        }
        
        return status_mapping.get(workbench_status.upper())
    
    async def update_pipeline_run_status(self, pipeline_run_id: int, new_status: str, workbench_status: Dict[str, Any]):
        """Update pipeline run status in database"""
        try:
            url = f"{DATABASE_API_BASE_URL}/pipeline-runs/{pipeline_run_id}/status"
            
            payload = {
                "status": new_status,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            # Add completion time for terminal states
            if new_status in ["COMPLETED", "FAILED", "CANCELLED"]:
                payload["completed_at"] = datetime.now(timezone.utc).isoformat()
            
            # Add error message if failed
            if new_status == "FAILED" and workbench_status.get("error_message"):
                payload["error_message"] = workbench_status.get("error_message")
            
            response = requests.put(url, json=payload, timeout=10)
            
            if response.status_code in [200, 204]:
                logger.info(f"Successfully updated pipeline_run {pipeline_run_id} status to {new_status}")
            else:
                logger.error(f"Failed to update pipeline_run status: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error updating pipeline_run {pipeline_run_id} status: {e}")
    
    def stop(self):
        """Stop the monitoring worker"""
        logger.info("Stopping Batch Monitor Worker...")
        self.running = False

# Health check endpoint for the worker
from fastapi import FastAPI

app = FastAPI(title="Batch Monitor Worker")

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "batch-monitor-worker"}

@app.get("/status")
def worker_status():
    """Get worker status"""
    return {
        "status": "running" if worker.running else "stopped",
        "poll_interval": POLL_INTERVAL,
        "database_api": DATABASE_API_BASE_URL,
        "workbench_api": WORKBENCH_API_BASE_URL
    }

# Global worker instance
worker = BatchMonitorWorker()

@app.on_event("startup")
async def startup_event():
    """Start the worker when the app starts"""
    asyncio.create_task(worker.start())

@app.on_event("shutdown")
async def shutdown_event():
    """Stop the worker when the app shuts down"""
    worker.stop()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
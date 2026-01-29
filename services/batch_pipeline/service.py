from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import requests
import logging
from datetime import datetime, timezone
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

service = FastAPI(title="Batch Pipeline Service")

# Configuration - these should be environment variables in production
WORKBENCH_API_BASE_URL = "http://workbench-api:8080"  # You'll provide this
DATABASE_API_BASE_URL = "http://ingestion-api:8000"   # To update pipeline run status

class BatchPipelineRequest(BaseModel):
    pipeline_run_id: int
    input_path_prefix: str
    output_path_prefix: str
    report_path_prefix: Optional[str] = None

class WorkbenchJobRequest(BaseModel):
    request_id: str
    input_path: str
    output_path: str
    report_path: Optional[str] = None
    pipeline_config: Optional[Dict[str, Any]] = None

class WorkbenchJobResponse(BaseModel):
    workbench_job_id: str
    status: str
    message: Optional[str] = None

@service.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "batch-pipeline"}

@service.post("/execute-batch")
async def execute_batch_pipeline(request: BatchPipelineRequest):
    try:
        logger.info(f"Starting batch pipeline execution for pipeline_run_id: {request.pipeline_run_id}")
        
        # Step 1: Get pipeline run details from database
        pipeline_run_details = await get_pipeline_run_details(request.pipeline_run_id)
        if not pipeline_run_details:
            raise HTTPException(status_code=404, detail=f"Pipeline run {request.pipeline_run_id} not found")
        
        pipeline_details = pipeline_run_details.get("pipeline")
        request_id = pipeline_details.get("request_id")
        
        if not request_id:
            raise HTTPException(status_code=400, detail="Pipeline request_id is required for batch processing")
        
        logger.info(f"Found request_id: {request_id} for pipeline_run_id: {request.pipeline_run_id}")
        
        # Step 2: Update status to RUNNING
        await update_pipeline_run_status(request.pipeline_run_id, "RUNNING")
        
        # Step 3: Prepare workbench API request
        workbench_request = WorkbenchJobRequest(
            request_id=request_id,
            input_path=request.input_path_prefix,
            output_path=request.output_path_prefix,
            report_path=request.report_path_prefix,
            pipeline_config={
                "pipeline_run_id": request.pipeline_run_id,
                "workflow_name": pipeline_details.get("workflow_name"),
                "mode": "BATCH"
            }
        )
        
        # Step 4: Call workbench API
        workbench_response = await call_workbench_api(workbench_request)
        
        # Step 5: Update pipeline run with workbench job ID
        await update_pipeline_run_workbench_id(
            request.pipeline_run_id, 
            workbench_response.workbench_job_id
        )
        
        logger.info(f"Batch pipeline submitted successfully. Workbench job ID: {workbench_response.workbench_job_id}")
        
        return {
            "status": "success",
            "pipeline_run_id": request.pipeline_run_id,
            "workbench_job_id": workbench_response.workbench_job_id,
            "message": "Batch pipeline submitted to workbench API"
        }
        
    except Exception as e:
        logger.error(f"Error executing batch pipeline: {str(e)}")
        
        # Update status to FAILED
        try:
            await update_pipeline_run_status(request.pipeline_run_id, "FAILED", str(e))
        except Exception as update_error:
            logger.error(f"Failed to update pipeline run status: {update_error}")
        
        raise HTTPException(status_code=500, detail=f"Batch pipeline execution failed: {str(e)}")

async def get_pipeline_run_details(pipeline_run_id: int) -> Optional[Dict[str, Any]]:
    """Get pipeline run details from the database API"""
    try:
        url = f"{DATABASE_API_BASE_URL}/pipeline-runs/{pipeline_run_id}"
        response = requests.get(url)
        
        if response.status_code == 404:
            return None
        elif response.status_code != 200:
            raise Exception(f"Failed to get pipeline run details: {response.text}")
        
        return response.json()
        
    except Exception as e:
        logger.error(f"Error getting pipeline run details: {e}")
        raise

async def call_workbench_api(request: WorkbenchJobRequest) -> WorkbenchJobResponse:
    """Call the workbench API to submit the batch job"""
    try:
        # You'll need to provide the actual workbench API endpoint
        url = f"{WORKBENCH_API_BASE_URL}/submit-batch-job"
        
        payload = {
            "request_id": request.request_id,
            "input_path": request.input_path,
            "output_path": request.output_path,
            "report_path": request.report_path,
            "config": request.pipeline_config
        }
        
        logger.info(f"Calling workbench API: {url} with request_id: {request.request_id}")
        
        response = requests.post(url, json=payload, timeout=60)
        
        if response.status_code not in [200, 201, 202]:
            raise Exception(f"Workbench API call failed: {response.status_code} - {response.text}")
        
        result = response.json()
        
        return WorkbenchJobResponse(
            workbench_job_id=result.get("workbench_job_id") or result.get("job_id"),
            status=result.get("status", "SUBMITTED"),
            message=result.get("message")
        )
        
    except Exception as e:
        logger.error(f"Error calling workbench API: {e}")
        raise

async def update_pipeline_run_status(pipeline_run_id: int, status: str, error_message: Optional[str] = None):
    """Update pipeline run status in the database"""
    try:
        url = f"{DATABASE_API_BASE_URL}/pipeline-runs/{pipeline_run_id}/status"
        
        payload = {
            "status": status,
            "error_message": error_message,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        if status == "RUNNING":
            payload["started_at"] = datetime.now(timezone.utc).isoformat()
        elif status in ["COMPLETED", "FAILED", "CANCELLED"]:
            payload["completed_at"] = datetime.now(timezone.utc).isoformat()
        
        response = requests.put(url, json=payload)
        
        if response.status_code not in [200, 204]:
            logger.warning(f"Failed to update pipeline run status: {response.text}")
        else:
            logger.info(f"Updated pipeline run {pipeline_run_id} status to {status}")
            
    except Exception as e:
        logger.error(f"Error updating pipeline run status: {e}")

async def update_pipeline_run_workbench_id(pipeline_run_id: int, workbench_job_id: str):
    """Update pipeline run with workbench job ID"""
    try:
        url = f"{DATABASE_API_BASE_URL}/pipeline-runs/{pipeline_run_id}/workbench-job"
        
        payload = {
            "workbench_job_id": workbench_job_id,
            "status": "SUBMITTED",
            "started_at": datetime.now(timezone.utc).isoformat()
        }
        
        response = requests.put(url, json=payload)
        
        if response.status_code not in [200, 204]:
            logger.warning(f"Failed to update workbench job ID: {response.text}")
        else:
            logger.info(f"Updated pipeline run {pipeline_run_id} with workbench job ID: {workbench_job_id}")
            
    except Exception as e:
        logger.error(f"Error updating workbench job ID: {e}")

@service.get("/pipeline-runs/{pipeline_run_id}/status")
async def get_pipeline_run_status(pipeline_run_id: int):
    """Get current status of a pipeline run"""
    try:
        details = await get_pipeline_run_details(pipeline_run_id)
        if not details:
            raise HTTPException(status_code=404, detail="Pipeline run not found")
        
        return {
            "pipeline_run_id": pipeline_run_id,
            "status": details.get("status"),
            "workbench_job_id": details.get("workbench_job_id"),
            "started_at": details.get("started_at"),
            "completed_at": details.get("completed_at"),
            "error_message": details.get("error_message")
        }
        
    except Exception as e:
        logger.error(f"Error getting pipeline run status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
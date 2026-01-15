from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import requests
import pandas as pd
import tempfile
import logging
from datetime import datetime, timezone
import uuid
from minio_utils import MinIOManager
import json
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

service = FastAPI(title="Dynamic Ingestion API")

CONDUCTOR_URL = "http://conductor-server:8080/api"

minio_manager = MinIOManager()


class WorkflowRequest(BaseModel):
    workflowName: str
    workflowVersion: Optional[int] = 1
    csvMinioUri: Optional[str] = None
    csvContent: Optional[str] = None
    workflowInput: Optional[Dict[str, Any]] = {}  # any extra workflow input


# ----------------------------
# Helper functions
# ----------------------------
def read_csv(minio_uri: Optional[str], csv_content: Optional[str]) -> pd.DataFrame:
    """Read CSV from MinIO or string content into DataFrame."""
    if minio_uri:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
            local_path = tmp_file.name
            logger.info(f"Downloading CSV from MINIO: {minio_uri} to tempfile: {local_path}")
            minio_manager.download_file(minio_uri=minio_uri, local_path=local_path)
        df = pd.read_csv(local_path)
    elif csv_content:
        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))
    else:
        raise ValueError("Either minio_uri or csv_content must be provided")
    return df


def create_canonical_records(df: pd.DataFrame, workflow_name: str, job_id: str) -> List[Dict[str, Any]]:
    """Convert CSV rows into canonical workflow records."""
    canonical_records = []
    for idx, row in df.iterrows():
        record = {
            "job_id": job_id,
            "row_id": idx + 1,
            "input": row.to_dict(),
            "services": {},  # Each service will store its output here
            "meta": {
                "workflow": workflow_name,
                "status": "CREATED",
                "created_at": datetime.now(timezone.utc).isoformat()
            }
        }
        canonical_records.append(record)
    logger.info(f"Created {len(canonical_records)} canonical records for workflow: {json.dumps(canonical_records, indent=4)}")
    
    return canonical_records


def prepare_dynamic_workflow_input(workflow_input: Dict[str, Any], df: pd.DataFrame) -> Dict[str, Any]:
    """
    Attach canonical records and map all CSV columns dynamically.
    Ensures workflow tasks can reference any column without hardcoding.
    """
    workflow_input = workflow_input.copy() if workflow_input else {}
    workflow_input["jobId"] = str(uuid.uuid4())
    workflow_input["canonical_records"] = create_canonical_records(
        df, workflow_input.get("workflowName", "dynamic_workflow"), workflow_input["jobId"]
    )
    # Optional helper for Conductor dynamic references
    workflow_input["column_map"] = {col: f"${{workflow.input.canonical_records[*].input.{col}}}" for col in df.columns}
    logger.info("Column Map:\n%s", json.dumps(workflow_input["column_map"], indent=4))
    return workflow_input

def submit_workflow(workflow_name: str, workflow_input: Dict[str, Any], workflow_version: int = 1) -> Dict[str, Any]:
    """Submit workflow to Conductor."""
    url = f"{CONDUCTOR_URL}/workflow"
    payload = {
        "name": workflow_name,
        "input": workflow_input,
        "version": workflow_version
    }
    logger.info(f"Submitting workflow to Conductor: {url} with payload keys: {list(payload['input'].keys())}")
    response = requests.post(url, json=payload)
    
    logger.info(f"Conductor raw response status: {response.status_code}, body: {response.text!r}")
    
    if response.status_code not in [200, 202]:
        logger.error(f"Conductor workflow submission failed: {response.text}")
        raise HTTPException(status_code=500, detail=f"Failed to submit workflow: {response.text}")
    
    try:
        return response.json()
    except ValueError:
        logger.warning("Conductor response is not JSON, returning raw text")
        return {"raw_response": response.text, "status_code": response.status_code}



# ----------------------------
# API endpoint
# ----------------------------
@service.post("/submit_workflow")
def submit_workflow_endpoint(req: WorkflowRequest):
    try:
        # 1. Read CSV
        df = read_csv(req.csvMinioUri, req.csvContent)
        logger.info(f"CSV read successfully with {len(df)} records and columns: {list(df.columns)}")

        # 2. Prepare workflow input with canonical records
        workflow_input = prepare_dynamic_workflow_input(req.workflowInput, df)
        logger.info(f"Prepared dynamic workflow input with jobId: {workflow_input['jobId']}")

        # 3. Submit workflow to Conductor
        response = submit_workflow(req.workflowName, workflow_input, req.workflowVersion)
        logger.info(f"Workflow submitted successfully with response: {response}")

        return {
            "status": "success",
            "workflowId": response.get("workflowId"),
            "jobId": workflow_input["jobId"],
            "message": "Workflow submitted successfully",
        }

    except Exception as e:
        logger.error(f"Error submitting workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
from db.database import engine
from db.models import Base, PipelineRunStatus
from sqlalchemy.orm import Session
from fastapi import Depends
from db.database import get_db
from db.models import WorkflowBundle,PipelineRun,Pipeline,PipelineMode
from db.schemas import WorkflowBundleResponse,PipelineResponse,PipelineRunResponse
from workflow_registration import register_workflows_on_startup


Base.metadata.create_all(bind=engine)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

service = FastAPI(title="Dynamic Ingestion API")


@service.on_event("startup")
async def startup_event():
    """Run workflow registration on application startup."""
    logger.info("Ingestion API starting up...")
    
    # Register workflows automatically
    success = register_workflows_on_startup()
    
    if success:
        logger.info("All workflows registered successfully")
    else:
        logger.warning("Some workflows failed to register - check logs above")
    
    logger.info("Ingestion API startup completed")

CONDUCTOR_URL = "http://conductor-server:8080/api"

minio_manager = MinIOManager()

class WorkflowRequest(BaseModel):
    workflowName: str
    workflowVersion: Optional[int] = 1
    csvMinioUri: Optional[str] = None
    csvContent: Optional[str] = None
    workflowInput: Optional[Dict[str, Any]] = {} 

class WorkflowBundleCreate(BaseModel):
    bundle_name: str
    workflow_name: str
    workflow_version: int
    description: Optional[str] = None
    is_active: bool = True

class PipelineBundleCreate(BaseModel):
    workflow_name:str
    mode:str
    input_layout:Optional[int]=None
    output_layout:Optional[int]=None
    request_id:Optional[str]=None
    storage_type:Optional[str]=None
    storage_bucket_name:Optional[str]=None
    storage_input_path_prefix:Optional[str]=None
    storage_output_path_prefix:Optional[str]=None

class WorkflowBundleUpdate(BaseModel):
    bundle_name: Optional[str] = None
    workflow_name: Optional[str] = None
    workflow_version: Optional[int] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None

class PipelineRunRequest(BaseModel):
    pipeline_id:int
    input_path_prefix:Optional[str]=None
    output_path_prefix:Optional[str]=None
    report_path_prefix:Optional[str]=None
    realtime_workflow_request:Optional[WorkflowRequest]=None

def read_csv(minio_uri: Optional[str], csv_content: Optional[str]) -> pd.DataFrame:
    if minio_uri:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
            local_path = tmp_file.name
            logger.info(f"Downloading CSV from MINIO: {minio_uri} to tempfile: {local_path}")
            minio_manager.download_file(minio_uri=minio_uri, local_path=local_path)

        df = pd.read_csv(local_path, dtype=str, keep_default_na=False, na_values=[])
    elif csv_content:
        from io import StringIO
        df = pd.read_csv(StringIO(csv_content), dtype=str, keep_default_na=False, na_values=[])
    else:
        raise ValueError("Either minio_uri or csv_content must be provided")
    
    df = df.fillna('')
    
    return df


def create_canonical_records(df: pd.DataFrame, workflow_name: str, job_id: str) -> List[Dict[str, Any]]:
    import numpy as np
    
    canonical_records = []
    for idx, row in df.iterrows():
        row_dict = {}
        for key, value in row.to_dict().items():
            if pd.isna(value) or value is np.nan:
                row_dict[key] = ""
            elif isinstance(value, float) and not np.isfinite(value):
                row_dict[key] = ""
            else:
                row_dict[key] = str(value) if not pd.isna(value) else ""
        
        record = {
            "job_id": job_id,
            "row_id": idx + 1,
            "input": row_dict,
            "services": {},
            "meta": {
                "workflow": workflow_name,
                "status": "CREATED",
                "created_at": datetime.now(timezone.utc).isoformat()
            }
        }
        canonical_records.append(record)
    
    logger.info(f"Created {len(canonical_records)} canonical records for workflow")
    if canonical_records:
        logger.info(f"Sample canonical record: {json.dumps(canonical_records[0], indent=2)}")
    
    return canonical_records


def prepare_dynamic_workflow_input(workflow_input: Dict[str, Any], df: pd.DataFrame) -> Dict[str, Any]:
    workflow_input = workflow_input.copy() if workflow_input else {}
    workflow_input["jobId"] = str(uuid.uuid4())
    workflow_input["canonical_records"] = create_canonical_records(
        df, workflow_input.get("workflowName", "dynamic_workflow"), workflow_input["jobId"]
    )
    workflow_input["column_map"] = {col: f"${{workflow.input.canonical_records[*].input.{col}}}" for col in df.columns}
    logger.info("Column Map:\n%s", json.dumps(workflow_input["column_map"], indent=4))
    return workflow_input

def submit_workflow(workflow_name: str, workflow_input: Dict[str, Any], workflow_version: int = 1) -> Dict[str, Any]:
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


# @service.post("/submit_workflow")
def submit_workflow_endpoint(req: WorkflowRequest):
    try:
        df = read_csv(req.csvMinioUri, req.csvContent)
        logger.info(f"CSV read successfully with {len(df)} records and columns: {list(df.columns)}")

        workflow_input = prepare_dynamic_workflow_input(req.workflowInput, df)
        logger.info(f"Prepared dynamic workflow input with jobId: {workflow_input['jobId']}")

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



#This is the get api that we are gonna expose to list the active workflows that are available to us
@service.get(
    "/workflows",
    response_model=List[WorkflowBundleResponse]
)
def list_available_workflows(
    include_inactive: bool = False,
    db: Session = Depends(get_db)
):
    query = db.query(WorkflowBundle)

    if not include_inactive:
        query = query.filter(WorkflowBundle.is_active == True)

    workflows = query.order_by(WorkflowBundle.bundle_name).all()
    return workflows

#This it the get api that we are gonna expose to lisst the active workflows by bundleKey that are available to us.
@service.get("/workflows/{bundle_id}",response_model=WorkflowBundleResponse)
def get_workflow_by_bundle_key(bundle_id:int,db:Session=Depends(get_db)):
    workflow=db.query(WorkflowBundle).filter(WorkflowBundle.id==bundle_id).first()

    if not workflow:
        raise HTTPException(
            status_code=404,
            detail=f"Workflow bundle '{bundle_id}' not found"
        )
    
    if not workflow.is_active:
        raise HTTPException(
            status_code=400,
            detail=f"Workflow bundle '{bundle_id}' is inactive"
        )
    
    return workflow


@service.post("/bundles")
def create_workflow_bundle(payload:WorkflowBundleCreate,db:Session=Depends(get_db)):
    existing=(
        db.query(WorkflowBundle).filter(WorkflowBundle.bundle_name==payload.bundle_name).first()
    )

    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Workflow bundle '{payload.bundle_name}' already exists"
        )
    
    workflow_bundle=WorkflowBundle(
        bundle_name=payload.bundle_name,
        workflow_name=payload.workflow_name,
        workflow_version=payload.workflow_version,
        description=payload.description,
        is_active=payload.is_active
    )

    db.add(workflow_bundle)
    db.commit()
    db.refresh(workflow_bundle)
    return workflow_bundle

@service.post("/pipelines")
def create_pipeline_bundle(payload:PipelineBundleCreate,db:Session=Depends(get_db)):
    existing=(
        db.query(Pipeline).filter(Pipeline.workflow_name==payload.workflow_name).first()
    )

    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Pipeline with workflow name '{payload.workflow_name}' already exists"
        )
    
    pipeline=Pipeline(
        workflow_name=payload.workflow_name,
        mode=payload.mode,
        input_layout=payload.input_layout,
        output_layout=payload.output_layout,
        request_id=payload.request_id,
        storage_type=payload.storage_type,
        storage_bucket_name=payload.storage_bucket_name,
        storage_input_path_prefix=payload.storage_input_path_prefix,
        storage_output_path_prefix=payload.storage_output_path_prefix
    )

    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)
    return pipeline


@service.put("/bundles/{bundle_id}")
def update_workflow_bundle(bundle_id:int,payload:WorkflowBundleUpdate,db:Session=Depends(get_db)):
    workflow=(
        db.query(WorkflowBundle).filter(WorkflowBundle.id==bundle_id).first()
    )

    if not workflow:
        raise HTTPException(
            status_code=404,
            detail=f"Workflow bundle with ID '{bundle_id}' not found"
        )

    if payload.bundle_name is not None:
        workflow.bundle_name = payload.bundle_name

    if payload.workflow_name is not None:
        workflow.workflow_name = payload.workflow_name

    if payload.workflow_version is not None:
        workflow.workflow_version = payload.workflow_version

    if payload.description is not None:
        workflow.description = payload.description
    
    if payload.is_active is not None:
        workflow.is_active = payload.is_active

    db.commit()
    db.refresh(workflow)
    return workflow


#List all Pipelines
@service.get("/pipelines",response_model=List[PipelineResponse])
def list_pipelines(include_inactive: bool = False,db: Session = Depends(get_db)):
    query=db.query(Pipeline)
    if not include_inactive:
        query=query.filter(Pipeline.is_active==True)
    pipelines=query.order_by(Pipeline.id).all()
    return pipelines

#List Pipelineby id
@service.get("/pipelines/{pipeline_id}", response_model=PipelineResponse)
def get_pipeline_by_id(pipeline_id: int, db: Session = Depends(get_db)):
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(
            status_code=404,
            detail=f"Pipeline with ID '{pipeline_id}' not found"
        )
    return pipeline


#Post endpoint to create a pipeline run and submit it to the conductor workflow
@service.post("/pipeline-run", response_model=PipelineRunResponse)
def create_pipeline_run(payload: PipelineRunRequest, db: Session = Depends(get_db)):
    #Fetch the pipeline info
    pipeline = db.query(Pipeline).filter(Pipeline.id == payload.pipeline_id).first()

    if not pipeline:
        raise HTTPException(
            status_code=404,
            detail=f"Pipeline with ID '{payload.pipeline_id}' not found"
        )

    # Create a new pipeline run
    pipeline_run = PipelineRun(
        pipeline_id=pipeline.id,
        user_storage_input_path_prefix=payload.input_path_prefix,
        user_storage_output_path_prefix=payload.output_path_prefix,
        user_storage_report_path_prefix=payload.report_path_prefix
    )

    db.add(pipeline_run)
    db.commit()
    db.refresh(pipeline_run)
    logger.info(f"Pipeline run created with id {pipeline_run.id} for pipeline {pipeline.id}")

    if pipeline.mode == PipelineMode.BATCH:
        # Call batch pipeline service
        batch_payload = {
            "pipeline_run_id": pipeline_run.id,
            "request_id": pipeline.request_id,
            "input_path_prefix": payload.input_path_prefix,
            "output_path_prefix": payload.output_path_prefix,
            "report_path_prefix": payload.report_path_prefix
        }
        
        # Submit to Conductor workflow that will call batch pipeline service
        conductor_payload = {
            "name": "batch_pipeline_workflow",
            "input": batch_payload
        }

        try:
            conductor_url = f"{CONDUCTOR_URL}/workflow"
            response = requests.post(conductor_url, json=conductor_payload)
            
            if response.status_code not in [200, 202]:
                logger.error(f"Failed to submit batch workflow: {response.text}")
                pipeline_run.status = PipelineRunStatus.FAILED
                pipeline_run.error_message = f"Conductor submission failed: {response.text}"
                db.commit()
                raise HTTPException(status_code=500, detail="Failed to submit batch workflow")
            
            # Update status to SUBMITTED
            pipeline_run.status = PipelineRunStatus.SUBMITTED
            pipeline_run.started_at = datetime.now(timezone.utc)
            db.commit()
            db.refresh(pipeline_run)
            
            logger.info(f"Batch pipeline submitted successfully for pipeline_run_id: {pipeline_run.id}")
            
        except Exception as e:
            logger.error(f"Error submitting batch pipeline: {e}")
            pipeline_run.status = PipelineRunStatus.FAILED
            pipeline_run.error_message = str(e)
            db.commit()
            raise HTTPException(status_code=500, detail=f"Failed to submit batch pipeline: {str(e)}")

    elif pipeline.mode == PipelineMode.REALTIME:
        if not payload.realtime_workflow_request:
            raise HTTPException(
                status_code=400,
                detail="realtime_workflow_request is required for REALTIME pipelines"
            )
        
        # Execute realtime workflow
        try:
            realtime_response = submit_workflow_endpoint(payload.realtime_workflow_request)
            
            # Update pipeline run with realtime workflow info
            pipeline_run.status = PipelineRunStatus.SUBMITTED
            pipeline_run.workbench_job_id = realtime_response.get("workflowId")
            pipeline_run.started_at = datetime.now(timezone.utc)
            db.commit()
            db.refresh(pipeline_run)
            
            logger.info(f"Realtime workflow submitted successfully for pipeline_run_id: {pipeline_run.id}")
            
        except Exception as e:
            logger.error(f"Error submitting realtime workflow: {e}")
            pipeline_run.status = PipelineRunStatus.FAILED
            pipeline_run.error_message = str(e)
            db.commit()
            raise HTTPException(status_code=500, detail=f"Failed to submit realtime workflow: {str(e)}")

    return pipeline_run
    
    



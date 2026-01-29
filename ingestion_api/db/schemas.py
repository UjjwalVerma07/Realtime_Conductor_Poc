from datetime import datetime
from pydantic import BaseModel
from typing import Optional,Dict,Any

class WorkflowBundleResponse(BaseModel):
    id: int
    bundle_name: str
    workflow_name: str
    workflow_version: int
    description: Optional[str] = None
    is_active: bool

 
    class Config:
        from_attributes = True

class PipelineResponse(BaseModel):
    id:int
    workflow_name:str
    mode:str
    input_layout:Optional[int]=None
    output_layout:Optional[int]=None
    request_id:Optional[str]=None
    storage_type:Optional[str]=None
    storage_bucket_name:Optional[str]=None
    storage_input_path_prefix:Optional[str]=None
    storage_output_path_prefix:Optional[str]=None
    is_active:bool

    class Config:
        from_attributes=True


class PipelineRunResponse(BaseModel):
    id:int
    pipeline_id:int
    status:str
    workbench_job_id:Optional[str]=None
    error_message:Optional[str]=None
    user_storage_input_path_prefix:Optional[str]=None
    user_storage_output_path_prefix:Optional[str]=None
    user_storage_report_path_prefix:Optional[str]=None
    started_at:Optional[datetime]=None
    completed_at:Optional[datetime]=None

    class Config:
        from_attributes=True
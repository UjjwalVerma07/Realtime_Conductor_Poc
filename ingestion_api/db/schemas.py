from pydantic import BaseModel
from typing import Optional

class WorkflowBundleResponse(BaseModel):
    id: int
    bundle_name: str
    workflow_name: str
    workflow_version: int
    description: Optional[str] = None
    is_active: bool


    class Config:
        from_attributes = True

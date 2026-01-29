from sqlalchemy import Column, Integer,String,Boolean,Text,DateTime
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid
from .database import Base
from sqlalchemy import (
    Column, BigInteger, String, Text,
    Enum, Boolean, DateTime,ForeignKey
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from db.database import Base
import enum


class WorkflowBundle(Base):
    __tablename__="workflow_bundles"
    
    id=Column(Integer,primary_key=True,index=True)
    bundle_name=Column(String(255),unique=True,nullable=False)
    workflow_name=Column(String(255),nullable=False)
    workflow_version=Column(Integer,nullable=False,default=1)
    description=Column(Text,nullable=True)
    is_active=Column(Boolean,default=True,nullable=False)
    created_at=Column(DateTime,default=datetime.utcnow,nullable=False)
    updated_at=Column(DateTime,default=datetime.utcnow,onupdate=datetime.utcnow,nullable=False)


class PipelineMode(str, enum.Enum):
    REALTIME = "REALTIME"
    BATCH = "BATCH"


class Pipeline(Base):
    __tablename__="pipelines"

    id=Column(Integer,primary_key=True,index=True)
    workflow_name=Column(String(255),unique=True,nullable=False)
    mode=Column(Enum(PipelineMode),nullable=False)
    input_layout=Column(Integer,nullable=True)
    output_layout=Column(Integer,nullable=True)

    request_id=Column(String(255),nullable=True)
    description=Column(String(255),nullable=True)
    storage_type=Column(String(255),nullable=True)
    storage_bucket_name=Column(String(255),nullable=True)

    storage_input_path_prefix=Column(String(1024),nullable=True)
    storage_output_path_prefix=Column(String(1024),nullable=True)

    is_active=Column(Boolean,default=True,nullable=False)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )



class PipelineRunStatus(str, enum.Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class PipelineRun(Base):
    __tablename__="pipeline_runs"
    id=Column(Integer,primary_key=True,index=True)
    pipeline_id=Column(Integer,ForeignKey("pipelines.id",ondelete="CASCADE"),nullable=False)
    status = Column(
        Enum(PipelineRunStatus),
        nullable=False,
        default=PipelineRunStatus.CREATED
    )
    workbench_job_id=Column(String(255),nullable=True)
    error_message = Column(Text, nullable=True)
    user_storage_input_path_prefix=Column(String(1024),nullable=True)
    user_storage_output_path_prefix=Column(String(1024),nullable=True)
    user_storage_report_path_prefix=Column(String(1024),nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )

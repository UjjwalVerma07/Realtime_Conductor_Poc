from sqlalchemy import Column, Integer,String,Boolean,Text,DateTime
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid
from .database import Base


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



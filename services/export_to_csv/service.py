from fastapi import FastAPI
from pydantic import BaseModel
from typing import List,Dict,Any,Optional
import requests
import pandas as pd
import io
import uuid
import csv
import logging
from minio_utils import MinIOManager
import json
import os
import tempfile

logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s")
logger=logging.getLogger(__name__)

service=FastAPI(title="Export to CSV Service")

CONDUCTOR_URL = "http://conductor-server:8080/api"

minio_manager=MinIOManager()

MINIO_BUCKET=os.getenv("MINIO_BUCKET","workflow-output")

class ExportTaskInput(BaseModel):
    canonical_records:List[Dict[str,Any]]
    jobId:str

def flatten_dict(d:Dict[str,Any],parent_key:str="",sep:str="_")->Dict[str,Any]:
    """Recursively flattens nested Dicitionary"""
    items={}
    for key,value in d.items():
        new_key=f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value,dict):
            items.update(flatten_dict(value,new_key,sep=sep))
        else:
            items[new_key]=value
    return items


def flatten_canonical_record(record:Dict[str,Any])->Dict[str,Any]:
    """
    Flattens a single record into the csv ready dicitonary
    """
    flat_record={}
    flat_record["job_id"]=record.get("job_id","")
    flat_record["row_id"]=record.get("row_id","")

    input_data=record.get("input",{})
    for key,value in input_data.items():
        flat_record[f"input_{key}"]=value

    #Service Output:-
    services=record.get("services",{})
    for service_name,service_payload in services.items():
        flattened_service=flatten_dict(service_payload)
        for k,v in flattened_service.items():
            flat_record[f"{service_name}_{k}"]=v

    return flat_record           

    

@service.post("/process")
def process_export_task(task_input:ExportTaskInput):
    logger.info(f"Received export task for jobId: {task_input.jobId} with len of canonical_records: {len(task_input.canonical_records)}")

    #Flatten all canonical records
    flattened_records=[]

    for rec in task_input.canonical_records:
        flattened_records.append(flatten_canonical_record(rec))

    #Collect all possible CSV headers
    fieldnames=set()
    for record in flattened_records:
        fieldnames.update(record.keys())

    fieldnames=sorted(fieldnames)


    with tempfile.NamedTemporaryFile(mode="w",newline="",encoding="utf-8",suffix=".csv",delete=False) as tmp_file:
        writer = csv.DictWriter(tmp_file, fieldnames=fieldnames)
        writer.writeheader()
        for record in flattened_records:
            writer.writerow(record)

        temp_file_path = tmp_file.name
    
    object_key = f"{task_input.jobId}/final_output.csv"
    minio_manager.upload_file(temp_file_path,MINIO_BUCKET,object_key)

    os.remove(temp_file_path)
    return {
        "status": "SUCCESS",
        "jobId": task_input.jobId,
        "records_exported": len(flattened_records),
        "output_uri": f"minio://{MINIO_BUCKET}/{object_key}"
    }



    

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
    canonical_records: List[Dict[str,Any]]
    jobId: str
    output_field_selection: Dict[str,Any] = {}

def flatten_dict(d:Dict[str,Any],parent_key:str="",sep:str="_")->Dict[str,Any]:
    items={}
    for key,value in d.items():
        new_key=f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value,dict):
            items.update(flatten_dict(value,new_key,sep=sep))
        else:
            items[new_key]=value
    return items


def get_nested_value(data: dict, field_path: str):
    keys = field_path.split('.')
    value = data
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    return value


def filter_service_output(service_name: str, service_data: Dict[str, Any], field_selection: Dict[str, Any]) -> Dict[str, Any]:
    if service_name not in field_selection or not field_selection[service_name].get("enabled", True):
        return {}
    
    selected_fields = field_selection[service_name].get("fields", {})
    filtered_output = {}
    

    for field_path, include_field in selected_fields.items():
        if include_field:
            # Handle nested paths like 'details.reference' or 'output.person1_firstname'
            if '.' in field_path:
                value = get_nested_value(service_data, field_path)
                if value is not None:
                    # Create nested structure in output
                    keys = field_path.split('.')
                    current = filtered_output
                    for key in keys[:-1]:
                        if key not in current:
                            current[key] = {}
                        current = current[key]
                    current[keys[-1]] = value
            else:
                # Handle top-level fields
                if field_path in service_data:
                    filtered_output[field_path] = service_data[field_path]
    
    return filtered_output


def flatten_canonical_record_with_selection(record: Dict[str, Any], field_selection: Dict[str, Any]) -> Dict[str, Any]:
    flat_record = {}
    flat_record["job_id"] = record.get("job_id", "")
    flat_record["row_id"] = record.get("row_id", "")

    # Always include input data
    input_data = record.get("input", {})
    for key, value in input_data.items():
        flat_record[f"input_{key}"] = value

    # Service outputs with field selection
    services = record.get("services", {})
    for service_name, service_payload in services.items():
        # Filter service output based on user selection
        filtered_output = filter_service_output(service_name, service_payload, field_selection)
        
        # Flatten the filtered output
        if filtered_output:
            flattened_service = flatten_dict(filtered_output)
            for k, v in flattened_service.items():
                flat_record[f"{service_name}_{k}"] = v
        
        # Always include status for debugging
        if "status" in service_payload:
            flat_record[f"{service_name}_status"] = service_payload["status"]

    return flat_record           

    

@service.post("/process")
def process_export_task(task_input: ExportTaskInput):
    logger.info(f"Received export task for jobId: {task_input.jobId} with {len(task_input.canonical_records)} records")
    logger.info(f"Field selection config: {json.dumps(task_input.output_field_selection, indent=2)}")

    # Flatten all canonical records with field selection
    flattened_records = []
    for rec in task_input.canonical_records:
        flattened_records.append(flatten_canonical_record_with_selection(rec, task_input.output_field_selection))

    # Create ordered fieldnames instead of alphabetical sorting
    ordered_fieldnames = []
    
    ordered_fieldnames.extend(["job_id", "row_id"])

    input_fields = []
    for record in flattened_records:
        for key in record.keys():
            if key.startswith("input_") and key not in input_fields:
                input_fields.append(key)
    ordered_fieldnames.extend(sorted(input_fields))

    config_services = list(task_input.output_field_selection.keys()) if task_input.output_field_selection else []
    
    detected_services = set()
    for record in flattened_records:
        for key in record.keys():
            if not key.startswith("input_") and key not in ["job_id", "row_id"]:
                for service in config_services:
                    if key.startswith(f"{service}_"):
                        detected_services.add(service)
                        break
    
    service_order = []
    for service in config_services:
        if service in detected_services:
            service_order.append(service)

    for service in sorted(detected_services):
        if service not in service_order:
            service_order.append(service)
    
    logger.info(f"Auto-detected service execution order: {service_order}")
    

    service_fields = {service: [] for service in service_order}
    other_service_fields = []
    
    for record in flattened_records:
        for key in record.keys():
            if not key.startswith("input_") and key not in ["job_id", "row_id"]:

                service_found = False
                for service in service_order:
                    if key.startswith(f"{service}_"):
                        if key not in service_fields[service]:
                            service_fields[service].append(key)
                        service_found = True
                        break
                

                if not service_found and key not in other_service_fields:
                    other_service_fields.append(key)
    

    for service in service_order:
        if service_fields[service]:
         
            status_field = f"{service}_status"
            other_fields = [f for f in service_fields[service] if f != status_field]
            
            if status_field in service_fields[service]:
                ordered_fieldnames.append(status_field)
            ordered_fieldnames.extend(sorted(other_fields))

    ordered_fieldnames.extend(sorted(other_service_fields))
    
    fieldnames = ordered_fieldnames
    logger.info(f"Final CSV will have {len(fieldnames)} columns in logical order")
    logger.info(f"Column order preview: {fieldnames}...") 

    logger.info(f"Writing data to CSV file")
    with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", suffix=".csv", delete=False) as tmp_file:
        writer = csv.DictWriter(tmp_file, fieldnames=fieldnames)
        writer.writeheader()
        for record in flattened_records:
            writer.writerow(record)
        temp_file_path = tmp_file.name
    
    logger.info(f"CSV file created and uploading to MinIO: final_output.csv")
    object_key = f"{task_input.jobId}/final_output.csv"
    minio_manager.upload_file(temp_file_path, MINIO_BUCKET, object_key)

    os.remove(temp_file_path)
    return {
        "status": "SUCCESS",
        "jobId": task_input.jobId,
        "records_exported": len(flattened_records),
        "output_uri": f"minio://{MINIO_BUCKET}/{object_key}",
        "selected_fields_count": len(fieldnames),
        "column_order_preview": fieldnames  
    }



    

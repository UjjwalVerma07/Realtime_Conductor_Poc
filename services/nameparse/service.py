from fastapi import FastAPI,HTTPException,Request
from pydantic import BaseModel
from typing import List,Dict,Any,Union
import requests
import logging
import time
import json

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger=logging.getLogger(__name__)

service=FastAPI(title="NameParse service")
EXTERNAL_API_URL = "http://core-services-api.data-axle.com/v1/nua/microservices/nameparse"
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
TIMEOUT = 10  # seconds

class Header(BaseModel):
    tags:List[str]
    jobId:str
    systemId:str

class NameParseConfig(BaseModel):
    nameType:str="M"
    nameOrder:str="first-name-first"
    delimiter:str=","

class NameParseInput(BaseModel):
    header:Header
    request:Union[Dict[str, Any], List[Dict[str, Any]]]
    config:NameParseConfig=None


@service.post("/process")
def process_nameparse(data:NameParseInput,request:Request):
    logger.info(f"Received request from {request.client.host}:{data.json()}")
    records=data.request
    config=data.config.dict() if data.config else {}
    is_single=isinstance(records,dict)
    canonical_records=[records] if is_single else records

    requests_payload=[]

    for rec in canonical_records:
        try:
            name_record={"name":rec["input"]["name"]}
            name_record.update(config)
            requests_payload.append(name_record)
        except KeyError:
            raise HTTPException(
                status_code=400,
                detail="Invalid canonical record :name missing"
            )
    
    external_payload=None
    if is_single:
        external_payload={
            "header":data.header.dict(),
            "request":requests_payload[0]
        }
    else:
        external_payload={
            "header":data.header.dict(),
            "requests":requests_payload
        }
    
    logger.info(f"Calling external API with payload :{external_payload}")

    for attempt in range(1,MAX_RETRIES+1):
        try:
            response=requests.post(
                EXTERNAL_API_URL,
                json=external_payload,
                timeout=TIMEOUT
            )
            response.raise_for_status()
            api_response=response.json()
            logger.info(f"Received response from external API: {json.dumps(api_response,indent=4)}")
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt} failed : {e}")
            if attempt==MAX_RETRIES:
                raise HTTPException(
                    status_code=500,
                    detail=f"External API request failed after {MAX_RETRIES} attempts: {str(e)}"
                )
            time.sleep(RETRY_DELAY)
        

    api_results=[api_response] if is_single else api_response

    if len(api_results)!=len(canonical_records):
        raise HTTPException(
                status_code=500,
                detail="External API response does not match request"
            )
        
    for rec,result in zip(canonical_records,api_results):
        rec.setdefault("services",{})
        rec["services"]["nameparse"]={
                "status":"SUCCESS",
                "name":result.get("name"),
                "nameType":result.get("nameType"),
                "nameOrder":result.get("nameOrder"),
                "delimiter":result.get("delimiter"),
                "output":result.get("output"),
                "appendage":result.get("appendage")
            }

        rec.setdefault("meta",{})
        rec["meta"]["status"]="NAMEPARSE_COMPLETED"
        
    logger.info(f"Enriched canonical records:{json.dumps(canonical_records,indent=4)}")
    return canonical_records[0] if is_single else canonical_records

    


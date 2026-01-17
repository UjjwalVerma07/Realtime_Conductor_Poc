from fastapi import FastAPI,HTTPException,Request
from fastapi.responses import JSONResponse
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
TIMEOUT = 60  # seconds

# Mock mode for testing - remove this in production
MOCK_MODE = False

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

    # -------- call external API or use mock --------
    if MOCK_MODE:
        # Mock response for testing
        logger.info("Using mock mode for nameparse")
        if is_single:
            api_response = {
                "name": requests_payload[0]["name"],
                "nameType": requests_payload[0].get("nameType", "M"),
                "nameOrder": requests_payload[0].get("nameOrder", "first-name-first"),
                "delimiter": requests_payload[0].get("delimiter", ","),
                "output": {
                    "firstName": requests_payload[0]["name"].split()[0] if " " in requests_payload[0]["name"] else requests_payload[0]["name"],
                    "lastName": requests_payload[0]["name"].split()[-1] if " " in requests_payload[0]["name"] else ""
                },
                "appendage": ""
            }
        else:
            api_response = []
            for req in requests_payload:
                api_response.append({
                    "name": req["name"],
                    "nameType": req.get("nameType", "M"),
                    "nameOrder": req.get("nameOrder", "first-name-first"),
                    "delimiter": req.get("delimiter", ","),
                    "output": {
                        "firstName": req["name"].split()[0] if " " in req["name"] else req["name"],
                        "lastName": req["name"].split()[-1] if " " in req["name"] else ""
                    },
                    "appendage": ""
                })
    else:
        # Real API call
        logger.info(f"Making real API call to: {EXTERNAL_API_URL}")
        logger.info(f"Payload: {json.dumps(external_payload, indent=2)}")
        
        for attempt in range(1,MAX_RETRIES+1):
            try:
                logger.info(f"Attempt {attempt}/{MAX_RETRIES} - Calling external API...")
                
                response=requests.post(
                    EXTERNAL_API_URL,
                    json=external_payload,
                    timeout=TIMEOUT,
                    headers={
                        'Content-Type': 'application/json',
                        'User-Agent': 'NameParseService/1.0'
                    }
                )
                
                logger.info(f"Response status: {response.status_code}")
                logger.info(f"Response headers: {dict(response.headers)}")
                
                if response.status_code == 200:
                    api_response=response.json()
                    logger.info(f"Received response from external API: {json.dumps(api_response,indent=4)}")
                    break
                else:
                    logger.error(f"API returned status {response.status_code}: {response.text}")
                    response.raise_for_status()
                    
            except requests.exceptions.ConnectTimeout as e:
                logger.error(f"Connection timeout on attempt {attempt}: {e}")
            except requests.exceptions.ReadTimeout as e:
                logger.error(f"Read timeout on attempt {attempt}: {e}")
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error on attempt {attempt}: {e}")
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error on attempt {attempt}: {e}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Request exception on attempt {attempt}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt}: {e}")
                
            if attempt==MAX_RETRIES:
                logger.error(f"All {MAX_RETRIES} attempts failed")
                raise HTTPException(
                    status_code=500,
                    detail=f"External API request failed after {MAX_RETRIES} attempts"
                )
            
            logger.info(f"Waiting {RETRY_DELAY} seconds before retry...")
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
    
    response_data = {
        "status": "NAMEPARSE_COMPLETED",
        "processed": len(canonical_records)
    }
    
    logger.info(f"Returning response: {response_data}")
    
    # Return JSONResponse to ensure proper formatting and immediate response
    return JSONResponse(
        content=response_data,
        status_code=200,
        headers={"Content-Type": "application/json"}
    )

    


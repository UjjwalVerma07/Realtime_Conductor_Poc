from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Union
import requests
import logging
import json
import time

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

service = FastAPI(title="Email Hygiene service")

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 60

EXTERNAL_API_URL = "http://core-services-api.data-axle.com/v1/nua/microservices/email_hygiene"

# Mock response for testing - remove this in production
MOCK_MODE = False


class Header(BaseModel):
    tags: List[str]
    jobId: str


class EmailHygieneInput(BaseModel):
    header: Header
    request: Union[Dict[str, Any], List[Dict[str, Any]]]


@service.post("/process")
def process_email_hygiene(data: EmailHygieneInput, request: Request):
    logger.info(f"Received request from {request.client.host}")

    records = data.request
    is_single = isinstance(records, dict)
    canonical_records = [records] if is_single else records

    # -------- build external API payload --------
    emails = []
    record_index_map=[]
    for idx, rec in enumerate(canonical_records):
        try:
            emails.append({"email": rec["input"]["email"]})
            record_index_map.append(idx)
        except KeyError:
            rec.setdefault("services",{})
            rec["services"]["email_hygiene"] = {
                "status":"FAILED",
                "error":"Email Missing in Input"
            }


    if not emails:
        return {
            "status": "EMAIL_HYGIENE_COMPLETED",
            "canonical_records": canonical_records,
            "job_summary": {
                "service": "email_hygiene",
                "success_count": 0,
                "failure_count": len(canonical_records),
                "total": len(canonical_records)
            }
        }
    external_payload=None
    if is_single:
        external_payload = {
        "header": data.header.dict(),
        "request": emails[0] 
        }
    else:
        external_payload={
            "header":data.header.dict(),
            "requests":emails
        }


    logger.info(f"Calling external API: {external_payload}")

    # -------- call external API or use mock --------
    if MOCK_MODE:
        # Mock response for testing
        logger.info("Using mock mode for email hygiene")
        if is_single:
            api_response = {
                "status": "success",
                "input": emails[0]["email"],
                "details": {
                    "email": emails[0]["email"].upper(),
                    "reference": "00",
                    "indicator": "A"
                }
            }
        else:
            api_response = []
            for email in emails:
                api_response.append({
                    "status": "success",
                    "input": email["email"],
                    "details": {
                        "email": email["email"].upper(),
                        "reference": "00",
                        "indicator": "A"
                    }
                })
    else:
        # Real API call
        logger.info(f"Making real API call to: {EXTERNAL_API_URL}")
        logger.info(f"Payload: {json.dumps(external_payload, indent=2)}")
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Attempt {attempt}/{MAX_RETRIES} - Calling external API...")
                
                response = requests.post(
                    EXTERNAL_API_URL,
                    json=external_payload,
                    timeout=TIMEOUT,
                    headers={
                        'Content-Type': 'application/json',
                        'User-Agent': 'EmailHygieneService/1.0'
                    }
                )
                
                logger.info(f"Response status: {response.status_code}")
                logger.info(f"Response headers: {dict(response.headers)}")
                
                if response.status_code == 200:
                    api_response = response.json()
                    logger.info(f"Received response from external API: {json.dumps(api_response, indent=4)}")
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
                
            if attempt == MAX_RETRIES:
                logger.error(f"All {MAX_RETRIES} attempts failed")
                raise HTTPException(
                    status_code=500,
                    detail="Email hygiene external API failed after all retry attempts"
                )
            
            logger.info(f"Waiting {RETRY_DELAY} seconds before retry...")
            time.sleep(RETRY_DELAY)

    api_results = [api_response] if is_single else api_response

    if len(api_results) != len(canonical_records):
        raise HTTPException(
            status_code=500,
            detail="Response size mismatch from email hygiene API"
        )

    # -------- enrich canonical records --------
    success_count=0
    failure_count=0
    for rec, result in zip(canonical_records, api_results):
        rec.setdefault("services", {})
        if result.get("status")=="success":
            success_count+=1
            rec["services"]["email_hygiene"] = {
                "status": result.get("status"),
                "input": result.get("input"),
                "details": result.get("details")
            }
            rec.setdefault("meta", {})
            rec["meta"]["status"] = "EMAIL_HYGIENE_COMPLETED"
        else:
            failure_count+=1
            rec["services"]["email_hygiene"]={
                "status": "failed",
                "error": result.get("error","Unknown failure")
            }
            rec.setdefault("meta", {})
            rec["meta"]["status"] = "EMAIL_HYGIENE_FAILED"


    logger.info("Email hygiene enrichment completed")

    logger.info(f"Enriched canonical records: {json.dumps(canonical_records,indent=4)}")

    response_data = {
        "status": "EMAIL_HYGIENE_COMPLETED",
        "processed": len(canonical_records)
    }
    
    logger.info(f"Returning response: {response_data}")
    
    # Return JSONResponse to ensure proper formatting and immediate response
    # return JSONResponse(
    #     content=response_data,
    #     status_code=200,
    #     headers={"Content-Type": "application/json"}
    # )
    return {
        "status":"EMAIL_HYGIENE_COMPLETED",
        "canonical_records": canonical_records,
        "job_summary":{
            "service":"email_hygiene",
            "success_count":success_count,
            "failure_count":failure_count,
            "total":len(canonical_records)
        }
    }
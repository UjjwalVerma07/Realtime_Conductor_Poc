from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Union
import requests
import logging
import json
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

service = FastAPI(title="US Address Lookup Service")

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 60

EXTERNAL_API_URL = "http://core-services-api.data-axle.com/v1/nua/microservices/us_address_lookup"

MOCK_MODE = False

class Header(BaseModel):
    tags: List[str]
    jobId: str
    systemId: str

class US_Address_Lookup(BaseModel):
    header: Header
    request: Union[Dict[str, Any], List[Dict[str, Any]]]

@service.post("/process")
def process_us_address_lookup(data: US_Address_Lookup, request: Request):
    logger.info(f"Received request from {request.client.host}: {data.json()}")
    records = data.request
    is_single = isinstance(records, dict)
    canonical_records = [records] if is_single else records

    requests_payload = []

    for rec in canonical_records:
        try:
            address_record = {
                "name": rec["input"]["name"],
                "firm": rec["input"]["firm"],
                "address1": rec["input"]["address1"],
                "address2": rec["input"]["address2"],
                "lastline": rec["input"]["lastline"]
            }
            requests_payload.append(address_record)
        except KeyError:
            rec.setdefault("services", {})
            rec["services"]["us_address_lookup"] = {
                "status": "FAILED",
                "error": "Address or lastline is missing in input"
            }
        
    if not requests_payload:
        return {
            "status": "US_ADDRESS_COMPLETED",
            "canonical_records": canonical_records,
            "job_summary": {
                "service": "us_address_lookup",
                "success_count": 0,
                "failure_count": len(canonical_records),
                "total": len(canonical_records)
            }
        }

    external_payload = None
    if is_single:
        external_payload = {
            "header": data.header.dict(),
            "request": requests_payload[0]
        }
    else:
        external_payload = {
            "header": data.header.dict(),
            "requests": requests_payload
        }
    
    logger.info(f"Calling external API with payload: {external_payload}")

    # --------- call external API or use mock -----------------

    if MOCK_MODE:
        # Mock response for testing
        logger.info("Using Mock Mode for us_address_lookup")
        # Create mock response
        mock_response = {
            "output": {
                "standardized_address": {
                    "address1": requests_payload[0]["address1"],
                    "city": "Standardized City",
                    "state": "MA",
                    "postal": "01234"
                },
                "validation_status": "VALID"
            }
        }
        api_response = mock_response

    else:
        # Real API Call
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
                        'User-Agent': 'US_Address_Lookup/1.0'
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
                    detail=f"External API request failed after {MAX_RETRIES} attempts"
                )
            
            logger.info(f"Waiting {RETRY_DELAY} seconds before retry...")
            time.sleep(RETRY_DELAY)

    api_results = [api_response] if is_single else api_response

    if len(api_results) != len(canonical_records):
        raise HTTPException(
            status_code=500,
            detail="External API response does not match request"
        )
    
    success_count = 0
    failure_count = 0

    for rec, result in zip(canonical_records, api_results):
        rec.setdefault("services", {})
        if "output" in result:
            success_count += 1
            rec["services"]["us_address_lookup"] = {
                "status": "SUCCESS",
                "output": result.get("output")
            }

            rec.setdefault("meta", {})
            rec["meta"]["status"] = "US_ADDRESS_LOOKUP_COMPLETED"
        else:
            failure_count += 1
            rec["services"]["us_address_lookup"] = {
                "status": "FAILED",
                "error": result.get("error", "Unknown failure")
            }
            rec.setdefault("meta", {})
            rec["meta"]["status"] = "US_ADDRESS_LOOKUP_FAILED"

    logger.info(f"Enriched canonical records: {json.dumps(canonical_records, indent=4)}")
    
    return {
        "status": "US_ADDRESS_LOOKUP_COMPLETED",
        "canonical_records": canonical_records,
        "job_summary": {
            "service": "us_address_lookup",
            "success_count": success_count,
            "failure_count": failure_count,
            "total": len(canonical_records)
        }
    }
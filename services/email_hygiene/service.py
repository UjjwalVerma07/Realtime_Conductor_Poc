from fastapi import FastAPI, HTTPException, Request
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
TIMEOUT = 10

EXTERNAL_API_URL = "http://core-services-api.data-axle.com/v1/nua/microservices/email_hygiene"


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
    for rec in canonical_records:
        try:
            emails.append({"email": rec["input"]["email"]})
        except KeyError:
            raise HTTPException(
                status_code=400,
                detail="Invalid canonical record: email missing"
            )
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

    # -------- call external API --------
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                EXTERNAL_API_URL,
                json=external_payload,
                timeout=TIMEOUT
            )
            response.raise_for_status()
            api_response = response.json()
            logger.info(f"Received response from external API: {json.dumps(api_response, indent=4)}")
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt} failed: {e}")
            if attempt == MAX_RETRIES:
                raise HTTPException(
                    status_code=500,
                    detail="Email hygiene external API failed"
                )
            time.sleep(RETRY_DELAY)

    api_results = [api_response] if is_single else api_response

    if len(api_results) != len(canonical_records):
        raise HTTPException(
            status_code=500,
            detail="Response size mismatch from email hygiene API"
        )

    # -------- enrich canonical records --------
    for rec, result in zip(canonical_records, api_results):
        rec.setdefault("services", {})
        rec["services"]["email_hygiene"] = {
            "status": result.get("status"),
            "input": result.get("input"),
            "details": result.get("details")
        }

        rec.setdefault("meta", {})
        rec["meta"]["status"] = "EMAIL_HYGIENE_COMPLETED"

    logger.info("Email hygiene enrichment completed")

    logger.info(f"Enriched canonical records: {json.dumps(canonical_records,indent=4)}")

    return canonical_records[0] if is_single else canonical_records

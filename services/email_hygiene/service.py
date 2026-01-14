from fastapi import FastAPI,HTTPException,Request
from pydantic import BaseModel,Field,validator
from typing import List,Dict,Any,Union
import requests
import logging
import time
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger=logging.getLogger(__name__)

service=FastAPI(title="Email Hygiene service")

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
TIMEOUT = 10 

class Header(BaseModel):
    tags:List[str]
    jobId:str

class RequestData(BaseModel):
    email:str

class EmailHygieneInput(BaseModel):
    header:Header
    request:Union[RequestData,List[RequestData]]


EXTERNAL_API_URL = "http://core-services-api.data-axle.com/v1/nua/microservices/email_hygiene"

@service.post("/process")
def process_email_hygiene(data:EmailHygieneInput,request:Request):
    #Convert the pydantic model to dict
    logger.info(f"Received request from {request.client.host}: {data.json()}")
    payload=data.dict()
    # Normalize to list if a single record was sent
    if isinstance(data.request, RequestData):
        payload["request"] = data.request.dict()

    # Multiple records
    else:
        payload["requests"] = [r.dict() for r in data.request]

    logger.info(f"Normalized payload for external API: {payload}")
    attempt=0
    while(attempt<MAX_RETRIES):
          
        try:
            attempt+=1
            #Call the actual Email Hygiene RestApi
            logger.info(f"Calling external API: {EXTERNAL_API_URL} with payload:{payload}")

            response=requests.post(EXTERNAL_API_URL,json=payload,timeout=TIMEOUT)
            response.raise_for_status()

            logger.info(f"Received response from external API: {response.json()}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt failed :{attempt} with error {e}")
            if attempt<MAX_RETRIES:
                logger.info(f"Retrying in {RETRY_DELAY} seconds....")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Max retries reached . Failing the requests")
                raise HTTPException(status_code=500,detail=f"External API error after {MAX_RETRIES} retries: {e}")
        except ValueError as ve:
            logger.error(f"Invalid Input: {ve}")
            raise HTTPException(status_code=400,detail=f"Invalid input: {ve}")


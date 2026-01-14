from fastapi import FastAPI,HTTPException,Request
from pydantic import BaseModel
from typing import List,Dict,Any,Union
import requests
import logging
import time

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

class RequestData(BaseModel):
    name:str
    nameType:str="M"
    nameOrder:str="first-name-first"
    delimiter:str=","

class NameParseInput(BaseModel):
    header:Header
    request:Union[RequestData,List[RequestData]]


@service.post("/process")
def process_nameparse(data:NameParseInput,request:Request):
    logger.info(f"Received request from {request.client.host}:{data.json()}")
    payload=data.dict()

    attempt=0
    if isinstance(data.request,RequestData):
        payload["request"]=data.request.dict()
    else:
        payload["requests"]=[r.dict() for r in data.request]
    while(attempt<MAX_RETRIES):
        try:
            attempt+=1
            logger.info(f"Calling external API: {EXTERNAL_API_URL} with payload :{payload}")
            response=requests.post(EXTERNAL_API_URL,json=payload,timeout=TIMEOUT)
            
            if response.status_code==429:
                logger.warning(f"Rate limited by external API (429), Retry {attempt} after {RETRY_DELAY} seconds")
                if attempt<MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    raise HTTPException(status_code=429, detail="Rate limited by external API")
            
            if response.status_code!=200:
                logger.error(f"External API returned {response.status_code}: {response.text}")
                raise HTTPException(status_code=response.status_code,detail=response.text)
        
            logger.info(f"Received response from external API: {response.json()}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"RequestException on attempt {attempt}: {e}")
            if attempt < MAX_RETRIES:
                logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Max retries reached. Failing the request")
                raise HTTPException(status_code=500, detail=f"External API request failed after {MAX_RETRIES} retries: {e}")
    





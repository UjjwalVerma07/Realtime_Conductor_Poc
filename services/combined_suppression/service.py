from fastapi import FastAPI, HTTPException , Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List,Dict,Any,Union
import requests
import logging
import json 
import time

logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s")
logger=logging.getLogger(__name__)

service = FastAPI(title="Combined Suppression Service")

MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 60

EXTERNAL_API_URL = "http://core-services-api.data-axle.com/v1/nua/microservices/combined_suppression"

MOCK_MODE = False


class Header(BaseModel):
    tags:List[str]
    jobId:str
    systemId:str

class Combined_Suppression(BaseModel):
    header: Header
    request: Union[Dict[str,Any],List[Dict[str,Any]]]
    combined_suppression_config: Dict[str,Any] = {}


@service.post("/process")
def process_combined_suppression(data:Combined_Suppression,request:Request):
    logger.info(f"Received request from {request.client.host}:{data.json()}")

    records=data.request
    is_single=isinstance(records,dict)
    canonical_records=[records] if is_single else records
    
    # Get configuration from payload
    config = data.combined_suppression_config
    logger.info(f"Received combined_suppression_config: {json.dumps(config, indent=2)}")
    
    san_id = config.get("san_id", "10422243-522243-25")
    orientation = config.get("orientation", "FNF")
    name_type = config.get("nameType", "M")
    parameters = config.get("parameters", {})
    
    logger.info(f"Parsed config - san_id: {san_id}, orientation: {orientation}, name_type: {name_type}")
    logger.info(f"Parameters: {json.dumps(parameters, indent=2)}")
    
    requests_payload=[]

    for rec in canonical_records:
        try:
            combined_suppression_record = {
                "input": {
                    "standardkeys": {
                        "keys": {
                            "linkage": {
                                "dataset": "USERDEF",
                                "partition": 0,
                                "recordid": 1452172
                            },
                            "names": [
                                {
                                    "firstName": rec["input"]["name"].split(" ")[0] if rec["input"]["name"] else "",
                                    "lastName": rec["input"]["name"].split(" ")[1] if len(rec["input"]["name"].split(" ")) > 1 else ""
                                }
                            ],
                            "address": [
                                {
                                    "addressLine": [
                                        "",
                                        rec["input"]["address1"]
                                    ],
                                    "city": rec["input"]["city"],
                                    "state": rec["input"]["state"],
                                    "zipCode": rec["input"]["postal"],
                                    "zipFour": ""
                                }
                            ],
                            "orientation": orientation,
                            "nameType": name_type
                        },
                        "unparsed": {}
                    },
                    "ftcinput": {
                        "sanid": san_id
                    },
                    "email": rec["input"]["email"],
                    "telephone": rec["input"]["phone"]
                },
                "parameters": {
                    "suppressionFlags": {
                        "pandALL": parameters.get("suppressionFlags", {}).get("pandALL", False),
                        "MPSIndicator": parameters.get("suppressionFlags", {}).get("MPSIndicator", False),
                        "DTSIndicator": parameters.get("suppressionFlags", {}).get("DTSIndicator", False),
                        "OfficialIndicator": parameters.get("suppressionFlags", {}).get("OfficialIndicator", False),
                        "BUSIndicator": parameters.get("suppressionFlags", {}).get("BUSIndicator", False),
                        "DMIIndicator": parameters.get("suppressionFlags", {}).get("DMIIndicator", False),
                        "RETIndicator": parameters.get("suppressionFlags", {}).get("RETIndicator", False),
                        "EXTIndicator": parameters.get("suppressionFlags", {}).get("EXTIndicator", False),
                        "COLIndicator": parameters.get("suppressionFlags", {}).get("COLIndicator", False),
                        "MILIndicator": parameters.get("suppressionFlags", {}).get("MILIndicator", False),
                        "TRLIndicator": parameters.get("suppressionFlags", {}).get("TRLIndicator", False),
                        "NURIndicator": parameters.get("suppressionFlags", {}).get("NURIndicator", False),
                        "CLIIndicator": parameters.get("suppressionFlags", {}).get("CLIIndicator", False),
                        "DBAIndicator": parameters.get("suppressionFlags", {}).get("DBAIndicator", False),
                        "ACAIndicator": parameters.get("suppressionFlags", {}).get("ACAIndicator", False),
                        "Reserved": parameters.get("suppressionFlags", {}).get("Reserved", False),
                        "DECIndicator": parameters.get("suppressionFlags", {}).get("DECIndicator", False),
                        "RELIndicator": parameters.get("suppressionFlags", {}).get("RELIndicator", False)
                    },
                    "performNameAddress": parameters.get("performNameAddress", False),
                    "performEmail": parameters.get("performEmail", False),
                    "blankEmails": parameters.get("blankEmails", False),
                    "performPhone": parameters.get("performPhone", False),
                    "performFTC": parameters.get("performFTC", False),
                    "blankFTCPhones": parameters.get("blankFTCPhones", False),
                    "performAtty": parameters.get("performAtty", False),
                    "performTPS": parameters.get("performTPS", False),
                    "performBusinessPhone": parameters.get("performBusinessPhone", False)
                }
            }

            requests_payload.append(combined_suppression_record)

        except KeyError:
            rec.setdefault("services",{})
            rec["services"]["combined_suppression"]={
                "status":"FAILED",
                "error":"Input is missing in input"
            }
    if not requests_payload:
        return{
            "status":"Combined_Suppression_Completed",
            "canonical_records":canonical_records,
            "job_summary":{
                "service":"combined_suppression",
                "success_count":0,
                "failure_count":len(canonical_records),
                "total":len(canonical_records)
            }
        }
    
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
    logger.info(f"Calling external API with payload:{external_payload}")

    if MOCK_MODE:
        logger.info("Using Mock Mode for combined_suppression")
        mock_response={
            "output":{
                "_CS_AUDIT_recordid": "1",
                "_CS_AUDIT_Given_Initial": "K",
                "_CS_AUDIT_Given_Name": "Kristin",
                "_CS_AUDIT_Surname": "Cooper",
                "_CS_AUDIT_Gender": "",
                "_CS_AUDIT_Middle_Initial": "",
                "_CS_AUDIT_zip_code": "35004",
                "_CS_AUDIT_Street_Name": "WASHINGTON",
                "_CS_AUDIT_Primary_Number": "      1048",
                "_CS_AUDIT_Predirection": "",
                "_CS_AUDIT_Street_Designator": "DR",
                "_CS_AUDIT_Post_Direction": "",
                "_CS_AUDIT_Unit_Type": "",
                "_CS_AUDIT_Unit_Number": "",
                "_CS_AUDIT_State_Abbreviation": "AL",
                "_CS_AUDIT_Primary_Number_is_a_Box_Indicator": "N",
                "_CS_mps_pander": "N",
                "_CS_dts_pander": " ",
                "_CS_off_pander": " ",
                "_CS_bus_pander": "N",
                "_CS_dmi_pander": " ",
                "_CS_fds_pander": " ",
                "_CS_rel_pander": " ",
                "_CS_ext_pander": " ",
                "_CS_col_pander": "N",
                "_CS_mil_pander": "N",
                "_CS_trl_pander": "N",
                "_CS_ret_pander": "N",
                "_CS_nur_pander": "N",
                "_CS_cli_pander": " ",
                "_CS_dba_pander": " ",
                "_CS_aca_pander": "N",
                "_CS_email": "ANGELSORCE123@GMAIL.COM",
                "_CS_email_suppression": "N",
                "_CS_refresh_date": "         ",
                "_CS_supp_type": " ",
                "_CS_old_master_ind": " ",
                "_CS_ftc_match_indicator": "N",
                "_CS_ftc_run": "T",
                "_CS_ftc_date": "20251121",
                "_CS_tps_telephone": "N",
                "_CS_infousa_business_telephone": " ",
                "_CS_attorney_general_file": "N",
                "_CS_telephone": "2056406034"
            }
        }

        api_response=mock_response
    else:
        logger.info(f"Making real API call to : {EXTERNAL_API_URL}")
        logger.info(f"Payload: {json.dumps(external_payload,indent=2)}")

        for attempt in range(1,MAX_RETRIES+1):
            try:
                logger.info(f"Attempt {attempt}/{MAX_RETRIES}-Calling external API....")
                response = requests.post(
                    EXTERNAL_API_URL,
                    json=external_payload,
                    timeout=TIMEOUT,
                    headers={
                        'Content-Type': 'application/json',
                        'User-Agent': 'Combined_Suppression/1.0'
                    }
                )

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

    # Handle API response
    if is_single:
        api_results = [api_response]
    else:
        # For batch requests, the API should return a list of results
        api_results = api_response if isinstance(api_response, list) else [api_response]

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
            rec["services"]["combined_suppression"] = {
                "status": "SUCCESS",
                "output": result.get("output")
            }

            rec.setdefault("meta", {})
            rec["meta"]["status"] = "COMBINED_SUPPRESSION_COMPLETED"
        else:
            failure_count += 1
            rec["services"]["combined_suppression"] = {
                "status": "FAILED",
                "error": result.get("error", "Unknown failure")
            }
            rec.setdefault("meta", {})
            rec["meta"]["status"] = "COMBINED_SUPPRESSION_FAILED"

    logger.info(f"Enriched canonical records: {json.dumps(canonical_records, indent=4)}")
    
    return {
        "status": "COMBINED_SUPPRESSION_COMPLETED",
        "canonical_records": canonical_records,
        "job_summary": {
            "service": "combined_suppression",
            "success_count": success_count,
            "failure_count": failure_count,
            "total": len(canonical_records)
        }
    }


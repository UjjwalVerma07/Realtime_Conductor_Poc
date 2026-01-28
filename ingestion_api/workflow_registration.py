# ingestion_api/workflow_registration.py
import os
import json
import requests
import logging
from pathlib import Path
import time

logger = logging.getLogger(__name__)

# Use environment variable for Conductor URL
CONDUCTOR_URL = os.getenv("CONDUCTOR_SERVER", "http://conductor-server:8080/api")
WORKFLOW_REGISTRATION_ENDPOINT = f"{CONDUCTOR_URL}/metadata/workflow"

def wait_for_conductor(max_retries=60, delay=5):  # Increased from 30 retries, 2s delay
    """Wait for Conductor server to be ready."""
    # Use base URL for health check (without /api)
    base_url = CONDUCTOR_URL.replace('/api', '')
    health_url = f"{base_url}/health"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(health_url, timeout=10)  # Increased timeout
            if response.status_code == 200:
                logger.info("Conductor server is ready")
                return True
        except requests.exceptions.RequestException as e:
            logger.info(f"Waiting for Conductor server... (attempt {attempt + 1}/{max_retries}) - {str(e)}")
            time.sleep(delay)
    
    logger.error("Conductor server not available after maximum retries")
    return False

def register_workflows_on_startup():
    """Register all workflows during application startup."""
    logger.info("Starting automatic workflow registration...")
    
    # Wait for Conductor to be ready
    if not wait_for_conductor():
        logger.error("Cannot register workflows - Conductor not available")
        return False
    
    # Get workflow files from both mounted directories
    workflow_dirs = [
        Path("/app/workflows/bundled"),
        Path("/app/workflows/standalone")
    ]
    
    workflow_files = []
    for workflows_dir in workflow_dirs:
        if not workflows_dir.exists():
            logger.warning(f"Workflows directory not found: {workflows_dir}")
            continue
            
        logger.info(f"Scanning directory: {workflows_dir}")
        
        # Get all JSON files except payload files
        for file_path in workflows_dir.glob("*.json"):
            if "payload" in file_path.name.lower():
                logger.info(f"Skipping payload file: {file_path.name}")
                continue
            workflow_files.append(file_path)
            logger.info(f"Found workflow file: {file_path.name} in {workflows_dir.name}")
    
    if not workflow_files:
        logger.warning("No workflow files found to register")
        return False
    
    logger.info(f"Found {len(workflow_files)} workflow files to register")
    
    # Register each workflow
    success_count = 0
    failed_count = 0
    
    for file_path in workflow_files:
        try:
            with open(file_path, 'r') as f:
                workflow_def = json.load(f)
            
            workflow_name = workflow_def.get('name', 'Unknown')
            logger.info(f"Registering workflow: {workflow_name} from {file_path.name}")
            
            # Conductor expects an array of workflow definitions
            payload = [workflow_def]
            response = requests.put(WORKFLOW_REGISTRATION_ENDPOINT, json=payload, timeout=10)
            
            if response.status_code in [200, 204]:
                logger.info(f"Successfully registered workflow: {workflow_name}")
                success_count += 1
            else:
                logger.error(f"Failed to register workflow {workflow_name}: {response.status_code} - {response.text}")
                failed_count += 1
                
        except Exception as e:
            logger.error(f"Error processing workflow file {file_path}: {e}")
            failed_count += 1
    
    # Summary
    logger.info("=" * 50)
    logger.info("Workflow Registration Summary:")
    logger.info(f"Successfully registered: {success_count} workflows")
    logger.info(f"Failed to register: {failed_count} workflows")
    logger.info(f"Total files processed: {len(workflow_files)}")
    logger.info("=" * 50)
    
    return failed_count == 0

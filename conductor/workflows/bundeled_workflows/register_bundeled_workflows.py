#!/usr/bin/env python3
"""
Workflow Registration Script
Registers all workflow JSON files in the current directory to Conductor server.
Excludes workflow_payload_for_bundled_workflows.json as it's not a workflow definition.
"""

import os
import json
import requests
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Conductor server configuration
CONDUCTOR_URL = "http://localhost:8080/api"
WORKFLOW_REGISTRATION_ENDPOINT = f"{CONDUCTOR_URL}/metadata/workflow"

def load_workflow_file(file_path: str) -> dict:
    """Load and parse a workflow JSON file."""
    try:
        with open(file_path, 'r') as f:
            workflow_def = json.load(f)
        logger.info(f"Loaded workflow: {workflow_def.get('name', 'Unknown')} from {file_path}")
        return workflow_def
    except Exception as e:
        logger.error(f"Failed to load workflow file {file_path}: {e}")
        raise

def register_workflow(workflow_def: dict) -> bool:
    """Register a single workflow with Conductor."""
    workflow_name = workflow_def.get('name', 'Unknown')
    
    try:
        # Conductor expects an array of workflow definitions
        payload = [workflow_def]
        response = requests.put(WORKFLOW_REGISTRATION_ENDPOINT, json=payload)
        
        if response.status_code in [200, 204]:
            logger.info(f"Successfully registered workflow: {workflow_name}")
            return True
        else:
            logger.error(f"Failed to register workflow {workflow_name}: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error registering workflow {workflow_name}: {e}")
        return False

def get_workflow_files() -> list:
    """Get all workflow JSON files in current directory, excluding payload file."""
    current_dir = Path(__file__).parent
    workflow_files = []
    
    for file_path in current_dir.glob("*.json"):
        # Skip the payload file
        if file_path.name == "workflow_payload_for_bundled_workflows.json":
            logger.info(f"Skipping payload file: {file_path.name}")
            continue
            
        workflow_files.append(str(file_path))
        logger.info(f"Found workflow file: {file_path.name}")
    
    return workflow_files

def main():
    """Main function to register all workflows."""
    logger.info("Starting workflow registration process...")
    
    # Get all workflow files
    workflow_files = get_workflow_files()
    
    if not workflow_files:
        logger.warning("No workflow files found to register!")
        return
    
    logger.info(f"Found {len(workflow_files)} workflow files to register")
    
    # Register each workflow
    success_count = 0
    failed_count = 0
    
    for file_path in workflow_files:
        try:
            workflow_def = load_workflow_file(file_path)
            if register_workflow(workflow_def):
                success_count += 1
            else:
                failed_count += 1
        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")
            failed_count += 1
    
    # Summary
    logger.info("=" * 50)
    logger.info("Registration Summary:")
    logger.info(f"Successfully registered: {success_count} workflows")
    logger.info(f"Failed to register: {failed_count} workflows")
    logger.info(f"Total files processed: {len(workflow_files)}")
    
    if failed_count > 0:
        logger.warning("Some workflows failed to register. Check the logs above for details.")
        exit(1)
    else:
        logger.info("All workflows registered successfully!")

if __name__ == "__main__":
    main()
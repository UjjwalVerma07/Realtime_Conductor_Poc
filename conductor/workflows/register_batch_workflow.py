#!/usr/bin/env python3

import requests
import json
import sys
import os

def register_workflow(conductor_url, workflow_file):
    """Register a single workflow with Conductor"""
    try:
        with open(workflow_file, 'r') as f:
            workflow_def = json.load(f)
        
        # Register workflow definition
        url = f"{conductor_url}/metadata/workflow"
        response = requests.put(url, json=[workflow_def])
        
        if response.status_code in [200, 204]:
            print(f"✓ Successfully registered workflow: {workflow_def['name']}")
            return True
        else:
            print(f"✗ Failed to register workflow {workflow_def['name']}: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"✗ Error registering workflow {workflow_file}: {e}")
        return False

def main():
    conductor_url = os.getenv("CONDUCTOR_URL", "http://localhost:8080/api")
    
    # Register the batch pipeline workflow
    workflow_file = "batch_pipeline_workflow.json"
    
    if not os.path.exists(workflow_file):
        print(f"✗ Workflow file not found: {workflow_file}")
        sys.exit(1)
    
    print(f"Registering batch pipeline workflow with Conductor at {conductor_url}")
    
    success = register_workflow(conductor_url, workflow_file)
    
    if success:
        print("✓ Batch pipeline workflow registered successfully")
        sys.exit(0)
    else:
        print("✗ Failed to register batch pipeline workflow")
        sys.exit(1)

if __name__ == "__main__":
    main()
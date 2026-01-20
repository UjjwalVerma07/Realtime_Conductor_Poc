#!/usr/bin/env python3
"""
Upload the cleaned CSV file to MinIO
"""
import os
import sys
sys.path.append('ingestion_api')

from minio_utils import MinIOManager

def main():
    # Set MinIO environment variables
    os.environ["MINIO_ENDPOINT"] = "localhost:9000"
    os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
    os.environ["MINIO_SECRET_KEY"] = "minioadmin"
    os.environ["MINIO_USE_SSL"] = "false"
    
    # Initialize MinIO manager
    minio_manager = MinIOManager()
    
    # Upload the cleaned CSV file
    local_file = "final_50_entries_data.csv"
    bucket = "test-ingestion"
    object_key = "final_50_entries_data.csv"
    
    try:
        print(f"Uploading {local_file} to MinIO...")
        minio_uri = minio_manager.upload_file(local_file, bucket, object_key)
        print(f"✅ Successfully uploaded to: {minio_uri}")
        
        # Verify the file exists
        if minio_manager.file_exists(minio_uri):
            print(f"✅ File verified in MinIO: {minio_uri}")
        else:
            print(f"❌ File verification failed: {minio_uri}")
            
    except Exception as e:
        print(f"❌ Upload failed: {e}")

if __name__ == "__main__":
    main()
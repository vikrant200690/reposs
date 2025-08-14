import os
import json
import boto3
import requests
from dotenv import load_dotenv

load_dotenv()  # Loads variables from a local .env file if present

# Read required env vars
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")  # default if you like

CONTEXTUAL_API_KEY = os.getenv("CONTEXTUAL_API_KEY")
CONTEXTUAL_DATASTORE_ID = os.getenv("CONTEXTUAL_DATASTORE_ID")

# Validate required env vars
REQUIRED_VARS = {
    "S3_BUCKET": S3_BUCKET,
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "AWS_REGION": AWS_REGION,
    "CONTEXTUAL_API_KEY": CONTEXTUAL_API_KEY,
    "CONTEXTUAL_DATASTORE_ID": CONTEXTUAL_DATASTORE_ID,
}

missing = [k for k, v in REQUIRED_VARS.items() if not v]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

contextual_upload_url = f"https://api.contextual.ai/v1/datastores/{CONTEXTUAL_DATASTORE_ID}/documents"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

def upload_s3_to_contextual():
    response = s3.list_objects_v2(Bucket=S3_BUCKET)
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.lower().endswith(".pdf"):
            print(f"Uploading {key} from S3 to Contextual datastore")

            s3_object = s3.get_object(Bucket=S3_BUCKET, Key=key)
            file_bytes = s3_object["Body"].read()

            files = {
                "file": (key, file_bytes, "application/pdf"),
                "metadata": (
                    None,
                    json.dumps({
                        "custom_metadata": {
                            "source": "s3",
                            "s3_key": key
                        }
                    }),
                    "application/json",
                ),
            }

            headers = {
                "Authorization": f"Bearer {CONTEXTUAL_API_KEY}"
            }

            r = requests.post(
                contextual_upload_url,
                headers=headers,
                files=files,
                timeout=60,
            )

            if r.status_code in (200, 201):
                print(f"Uploaded {key}")
            else:
                print(f"Failed to upload {key}: {r.status_code} — {r.text}")

if __name__ == "__main__":
    upload_s3_to_contextual()

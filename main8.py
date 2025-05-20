import boto3
import requests
import uuid
import time
import os
from requests_aws4auth import AWS4Auth
from tabulate import tabulate
print("Write and read object in same region using X-Tigris-Consistent:true header")
# ---------- CONFIG ----------
region = "fra"
endpoint = "https://t3.storage.dev"
bucket = os.getenv("BUCKET", "tigris-consistency-test-bucket")
iterations = 10
file_size_bytes = 1024 * 1024
# ---------- AUTH ----------
session = boto3.Session()
credentials = session.get_credentials().get_frozen_credentials()
auth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    "auto",
    "s3",
    session_token=credentials.token
)
# ---------- Ensure bucket exists ----------
s3_client = session.client("s3", endpoint_url=endpoint)
if bucket not in [b["Name"] for b in s3_client.list_buckets()["Buckets"]]:
    s3_client.create_bucket(Bucket=bucket)
# ---------- Results ----------
results = []
for i in range(iterations):
    print("Iterations:", i + 1)
    object_key = f"strict-consistency-test-{uuid.uuid4()}"
    file_path = f"data-{uuid.uuid4()}.bin"
    with open(file_path, "wb") as f:
        f.write(os.urandom(file_size_bytes))
    put_url = f"{endpoint}/{bucket}/{object_key}"
    get_url = f"{put_url}?nocache={uuid.uuid4()}"
    # Step 1: PUT with region
    with open(file_path, "rb") as f:
        put_response = requests.put(put_url, data=f, auth=auth, headers={
            "X-Tigris-Regions": region,
            "X-Tigris-Consistent": "true",
        })
    expected_etag = put_response.headers.get("ETag", "").strip('"')
    # Step 2: GET with x-tigris-consistent:true
    with open(file_path, "rb") as f:
        expected_content = f.read()
    try:
        head_response = requests.head(get_url, auth=auth, headers={
            "X-Tigris-Regions": region,
            "X-Tigris-Consistent": "true",
        })
        actual_etag = head_response.headers.get("ETag", "").strip('"')
        if head_response.status_code == 200 and actual_etag == expected_etag:
            results.append((f"Run {i+1}", f"0 ms", "PASS"))
            get_response = requests.get(get_url, auth=auth, headers={
                "X-Tigris-Regions": region,
                "X-Tigris-Consistent": "true",
            })
            body = get_response.content
            if get_response.status_code != 200 or actual_etag != expected_etag or body != expected_content:
                raise Exception("Content mismatch")
        else:
            results.append((f"Run {i+1}", f"{elapsed:.2f} ms", "FAIL"))
    except Exception:
        results.append((f"Run {i+1}", "-", "ERROR"))
    os.remove(file_path)
# ---------- Output ----------
headers = ["Iteration", "GET Duration", "Status"]
print(tabulate(results, headers=headers, tablefmt="grid"))

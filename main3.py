import boto3
import requests
import uuid
import time
import os
from requests_aws4auth import AWS4Auth
from tabulate import tabulate
print("Delete object and read immediately from same region")
region = "fra"
endpoint = "https://t3.storage.dev"
bucket = os.getenv("BUCKET", "tigris-consistency-test-bucket")
poll_interval = 0.1  # 100ms
max_poll_seconds = 1
iterations = 10
file_size_bytes = 1024 * 1024  # 1MB
session = boto3.Session()
credentials = session.get_credentials().get_frozen_credentials()
auth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    "auto",
    "s3",
    session_token=credentials.token
)
s3_client = session.client("s3", endpoint_url=endpoint)
existing_buckets = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
if bucket not in existing_buckets:
    s3_client.create_bucket(Bucket=bucket)
results = []
for i in range(iterations):
    print("Iteration", i + 1)
    object_key = f"delete-test-{uuid.uuid4()}"
    file_path = f"upload-{uuid.uuid4()}.bin"
    with open(file_path, "wb") as f:
        f.write(os.urandom(file_size_bytes))
    put_url = f"{endpoint}/{bucket}/{object_key}"
    headers = {
        "X-Tigris-Regions": region,
    }
    with open(file_path, "rb") as f:
        put_response = requests.put(put_url, data=f, auth=auth, headers=headers)
    delete_response = requests.delete(put_url, headers=headers, auth=auth)
    head_response = requests.head(put_url, headers=headers, auth=auth)
    start = time.perf_counter()
    deadline = start + max_poll_seconds
    converged = False
    attempts = 0
    while time.perf_counter() < deadline:
        try:
            if head_response.status_code == 404:
                elapsed_ms = (time.perf_counter() - start) * 1000
                results.append((f"Run {i+1}", f"{elapsed_ms:.2f} ms", attempts, "PASS"))
                converged = True
                break
        except Exception as e:
            print("Error:", e)
        time.sleep(poll_interval)
        attempts += 1
        head_response = requests.head(put_url, headers=headers, auth=auth)
    if not converged:
        results.append((f"Run {i+1}", "TIMEOUT", attempts, "FAIL"))
    os.remove(file_path)
headers = ["Iteration", "Convergence Time", "Attempts", "Status"]
print(tabulate(results, headers=headers, tablefmt="grid"))

import boto3
import requests
import uuid
import time
import os
from requests_aws4auth import AWS4Auth
from tabulate import tabulate
# ---------- CONFIG ----------
region_put = "sjc"  # force write to SJC
endpoint = "https://t3.storage.dev"
bucket = "tigris-consistency-test-bucket"
poll_interval = 0.1  # 100ms
max_poll_seconds = 60
iterations = 10
file_size_bytes = 1024 * 1024  # 1MB
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
existing_buckets = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
if bucket not in existing_buckets:
    s3_client.create_bucket(Bucket=bucket)
# ---------- Results ----------
results = []
for i in range(iterations):
    object_key = f"global-replication-test-{uuid.uuid4()}"
    file_path = f"put-{uuid.uuid4()}.bin"
    with open(file_path, "wb") as f:
        f.write(os.urandom(file_size_bytes))
    put_url = f"{endpoint}/{bucket}/{object_key}"
    get_url = f"{put_url}?nocache={uuid.uuid4()}"
    # ---------- PUT to Region A (SJC) ----------
    put_headers = {
        "X-Tigris-Regions": region_put,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
    }
    with open(file_path, "rb") as f:
        put_response = requests.put(put_url, data=f, auth=auth, headers=put_headers)
    expected_etag = put_response.headers.get("ETag", "").strip('"')
    # ---------- Poll for convergence (default replica, no region header) ----------
    with open(file_path, "rb") as f:
        expected_content = f.read()
    start = time.perf_counter()
    deadline = start + max_poll_seconds
    attempts = 0
    converged = False
    while time.perf_counter() < deadline:
        attempts += 1
        try:
            head_resp = requests.head(get_url, auth=auth)
            etag = head_resp.headers.get("ETag", "").strip('"')
            size = int(head_resp.headers.get("Content-Length", -1))
            if etag == expected_etag and size == file_size_bytes:
                elapsed_ms = (time.perf_counter() - start) * 1000
                results.append((f"Run {i+1}", f"{elapsed_ms:.2f} ms", attempts, "PASS"))
                converged = True
                get_resp = requests.get(get_url, auth=auth)
                if get_resp.status_code != 200 or get_resp.content != expected_content:
                    raise Exception("Content mismatch")
                break
        except Exception:
            pass
        time.sleep(poll_interval)

import boto3
import requests
import uuid
import time
import os
from requests_aws4auth import AWS4Auth
from tabulate import tabulate
# ---------- CONFIG ----------
put_region = "sjc"
get_region = "fra"
endpoint = "https://t3.storage.dev"
bucket = "tigris-consistency-test-bucket"
iterations = 10
file_size_bytes = 1024 * 1024
max_attempts = 60
poll_interval = 1.0  # 1s polling
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
    object_key = f"overwrite-consistent-cross-region-{uuid.uuid4()}"
    file_path = f"data-{uuid.uuid4()}.bin"
    with open(file_path, "wb") as f:
        f.write(os.urandom(file_size_bytes))
    put_url = f"{endpoint}/{bucket}/{object_key}"
    get_url = f"{put_url}?nocache={uuid.uuid4()}"
    # Step 1: PUT (overwrite) to Region A
    with open(file_path, "rb") as f:
        put_response = requests.put(put_url, data=f, auth=auth, headers={
            "X-Tigris-Regions": put_region,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        })
    expected_etag = put_response.headers.get("ETag", "").strip('"')
    with open(file_path, "rb") as f:
        expected_content = f.read()
    # Step 2: Poll GET from Region B with strict consistency
    converged = False
    start = time.perf_counter()
    for attempt in range(max_attempts):
        try:
            get_response = requests.get(get_url, auth=auth, headers={
                "X-Tigris-Regions": get_region,
                "x-tigris-consistent": "true",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache"
            })
            actual_etag = get_response.headers.get("ETag", "").strip('"')
            if (
                get_response.status_code == 200 and
                actual_etag == expected_etag and
                get_response.content == expected_content
            ):
                elapsed = (time.perf_counter() - start) * 1000
                results.append((f"Run {i+1}", f"{elapsed:.2f} ms", attempt + 1, "PASS"))
                converged = True
                break
        except Exception:
            pass
        time.sleep(poll_interval)
    if not converged:
        results.append((f"Run {i+1}", "TIMEOUT", max_attempts, "FAIL"))
    os.remove(file_path)
# ---------- Output ----------
headers = ["Iteration", "Convergence Time", "Attempts", "Status"]
print(tabulate(results, headers=headers, tablefmt="grid"))

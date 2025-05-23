import boto3
import requests
import uuid
import time
import os
from requests_aws4auth import AWS4Auth
from tabulate import tabulate
#print("Overwrite object with X-Tigris-Consistent:true in Region A, read with same header in Region B")
print("Overwrite object with X-Tigris-Consistent:true in Region A, read with same header in Region A")
# ---------- CONFIG ----------
#put_region = "sjc"
#get_region = "fra"
endpoint = "https://t3.storage.dev"
bucket = os.getenv("BUCKET", "tigris-consistency-test-bucket")
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
    print("Iteration", i + 1)
    object_key = f"overwrite-consistent-cross-region-{uuid.uuid4()}"
    file_path = f"data-{uuid.uuid4()}.bin"
    with open(file_path, "wb") as f:
        f.write(os.urandom(file_size_bytes))
    put_url = f"{endpoint}/{bucket}/{object_key}"
    get_url = f"{put_url}"
    # Step 1: PUT (overwrite) to Region A
    with open(file_path, "rb") as f:
        put_response = requests.put(put_url, data=f, auth=auth, headers={
            "X-Tigris-Consistent": "true",
        })
    expected_etag = put_response.headers.get("ETag", "").strip('"')
    with open(file_path, "rb") as f:
        expected_content = f.read()
    # Step 2: Poll GET from Region B with strict consistency
    converged = False
    head_response = requests.head(get_url, auth=auth, headers={
        "X-Tigris-Consistent": "true",
    })
    start = time.perf_counter()
    for attempt in range(max_attempts):
        try:
            actual_etag = head_response.headers.get("ETag", "").strip('"')
            if head_response.status_code == 200 and actual_etag == expected_etag:
                elapsed = (time.perf_counter() - start) * 1000
                results.append((f"Run {i+1}", f"{elapsed:.2f} ms", attempt, "PASS"))
                converged = True
                get_response = requests.get(get_url, auth=auth, headers={
                    "X-Tigris-Consistent": "true",
                })
                actual_etag = get_response.headers.get("ETag", "").strip('"')
                if get_response.status_code != 200 or actual_etag != expected_etag or get_response.content != expected_content:
                    raise Exception("Content mismatch")
                break
        except Exception as e:
            print("Error:", e)
        time.sleep(poll_interval)
        attempts += 1
        head_response = requests.head(get_url, auth=auth, headers={
            "X-Tigris-Consistent": "true",
        })
    if not converged:
        results.append((f"Run {i+1}", "TIMEOUT", max_attempts, "FAIL"))
    os.remove(file_path)
# ---------- Output ----------
headers = ["Iteration", "Convergence Time", "Attempts", "Status"]
print(tabulate(results, headers=headers, tablefmt="grid"))

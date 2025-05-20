import boto3
import requests
import uuid
import time
import os
from tabulate import tabulate
from requests_aws4auth import AWS4Auth
print("Overwrite Object in Region A and Read from Region B")
# ---------- CONFIG ----------
region_put = "sjc"
endpoint = "https://t3.storage.dev"
bucket = os.getenv("BUCKET", "tigris-consistency-test-bucket")
poll_interval = 0.1  # 0.1 second
max_poll_seconds = 60
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
    print("Iteration", i + 1)
    object_key = f"overwrite-cross-region-test-{uuid.uuid4()}"
    url = f"{endpoint}/{bucket}/{object_key}"
    nocache_url = f"{url}?nocache={uuid.uuid4()}"
    # Step 1: Initial PUT to SJC
    file_initial = f"initial-{uuid.uuid4()}.bin"
    with open(file_initial, "wb") as f:
        f.write(os.urandom(file_size_bytes))
    with open(file_initial, "rb") as f:
        requests.put(url, data=f, auth=auth, headers={
            "X-Tigris-Regions": region_put,
        })
    # Step 2: Overwrite to SJC
    file_overwrite = f"overwrite-{uuid.uuid4()}.bin"
    with open(file_overwrite, "wb") as f:
        f.write(os.urandom(file_size_bytes))
    with open(file_overwrite, "rb") as f:
        resp = requests.put(url, data=f, auth=auth, headers={
            "X-Tigris-Regions": region_put,
        })
    expected_etag = resp.headers.get("ETag", "").strip('"')
    with open(file_overwrite, "rb") as f:
        expected_content = f.read()
    # Step 3: Start polling from global (FRA)
    head = requests.head(nocache_url, auth=auth)
    start = time.perf_counter()
    deadline = start + max_poll_seconds
    attempts = 0
    converged = False
    while time.perf_counter() < deadline:
        try:
            etag = head.headers.get("ETag", "").strip('"')
            size = int(head.headers.get("Content-Length", -1))
            if etag == expected_etag and size == file_size_bytes:
                elapsed = (time.perf_counter() - start) * 1000
                results.append((f"Run {i+1}", f"{elapsed:.2f} ms", attempts, "PASS"))
                converged = True
                get = requests.get(nocache_url, auth=auth)
                if get.status_code != 200 or get.content != expected_content:
                    raise Exception("Content mismatch")
                break
        except Exception as e:
            print("Error:", e)
        time.sleep(poll_interval)
        attempts += 1
        head = requests.head(nocache_url, auth=auth)
    if not converged:
        results.append((f"Run {i+1}", "TIMEOUT", attempts, "FAIL"))
    os.remove(file_initial)
    os.remove(file_overwrite)
# ---------- Print Table ----------
headers = ["Iteration", "Convergence Time", "Attempts", "Status"]
print(tabulate(results, headers=headers, tablefmt="grid"))

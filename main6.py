import boto3
import requests
import uuid
import time
import os
from requests_aws4auth import AWS4Auth
from tabulate import tabulate
print("Delete Object in Region A and Read from Region B")
# ---------- CONFIG ----------
region_put = "sjc"  # region to PUT and DELETE from
endpoint = "https://t3.storage.dev"
bucket = os.getenv("BUCKET", "tigris-consistency-test-bucket")
poll_interval = 0.1  # 1 second
max_poll_seconds = 600
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
    object_key = f"delete-cross-region-test-{uuid.uuid4()}"
    url = f"{endpoint}/{bucket}/{object_key}"
    file_path = f"delete-{uuid.uuid4()}.bin"
    # Step 1: Upload
    with open(file_path, "wb") as f:
        f.write(os.urandom(file_size_bytes))
    put_headers = {
        "X-Tigris-Regions": region_put,
    }
    with open(file_path, "rb") as f:
        put_resp = requests.put(url, data=f, auth=auth, headers=put_headers)
    if put_resp.status_code != 200:
        results.append((f"Run {i+1}", "PUT Failed", "-", "FAIL"))
        continue
    # Step 2: DELETE
    delete_resp = requests.delete(url, headers=put_headers, auth=auth)
    if delete_resp.status_code not in [204, 200]:
        results.append((f"Run {i+1}", "DELETE Failed", "-", "FAIL"))
        continue
    head = requests.head(url, auth=auth)
    start = time.perf_counter()
    deadline = start + max_poll_seconds
    attempts = 0
    converged = False
    # Step 3: Poll until GET and HEAD both 404 from global (FRA)
    while time.perf_counter() < deadline:
        try:
            if head.status_code == 404:
                elapsed = (time.perf_counter() - start) * 1000
                results.append((f"Run {i+1}", f"{elapsed:.2f} ms", attempts, "PASS"))
                converged = True
                break
        except Exception as e:
            print("Error:", e)
        time.sleep(poll_interval)
        attempts += 1
        head = requests.head(url, auth=auth)
    if not converged:
        results.append((f"Run {i+1}", "TIMEOUT", attempts, "FAIL"))
        break
    os.remove(file_path)
# ---------- Print Table ----------
headers = ["Iteration", "Convergence Time", "Attempts", "Status"]
print(tabulate(results, headers=headers, tablefmt="grid"))

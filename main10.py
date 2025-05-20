import boto3
import requests
import uuid
import time
import os
from threading import Thread
from requests_aws4auth import AWS4Auth
from tabulate import tabulate
# ---------- CONFIG ----------
endpoint = "https://t3.storage.dev"
bucket = "tigris-consistency-test-bucket"
put_regions = ["sjc", "fra"]
file_size_bytes = 1024 * 1024
iterations = 10
max_poll_seconds = 60
poll_interval = 1.0  # 1s
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
# ---------- PUT helper ----------
def put_object(region, file_path, url):
    with open(file_path, "rb") as f:
        requests.put(url, data=f, auth=auth, headers={
            "X-Tigris-Regions": region,
            "x-tigris-consistent": "true",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        })
# ---------- Results ----------
results = []
for i in range(iterations):
    object_key = f"concurrent-consistent-write-{uuid.uuid4()}"
    url = f"{endpoint}/{bucket}/{object_key}"
    files = {}
    contents = {}
    # Step 1: Create two unique 1MB files
    for region in put_regions:
        path = f"{region}-{uuid.uuid4()}.bin"
        with open(path, "wb") as f:
            f.write(os.urandom(file_size_bytes))
        with open(path, "rb") as f:
            contents[region] = f.read()
        files[region] = path
    # Step 2: Simultaneous PUTs with x-tigris-consistent: true
    threads = []
    for region in put_regions:
        t = Thread(target=put_object, args=(region, files[region], url))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    # Step 3: Poll until SJC and FRA converge to the same ETag and body
    converged = False
    start = time.perf_counter()
    deadline = start + max_poll_seconds
    attempts = 0
    while time.perf_counter() < deadline:
        time.sleep(poll_interval)
        attempts += 1
        try:
            res = {}
            etags = {}
            bodies = {}
            for region in put_regions:
                resp = requests.get(f"{url}?nocache={uuid.uuid4()}", auth=auth, headers={
                    "X-Tigris-Regions": region,
                    "x-tigris-consistent": "true",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache"
                })
                if resp.status_code == 200:
                    etags[region] = resp.headers.get("ETag", "").strip('"')
                    bodies[region] = resp.content
            if (
                len(etags) == 2 and
                etags[put_regions[0]] == etags[put_regions[1]] and
                bodies[put_regions[0]] == bodies[put_regions[1]]
            ):
                final = bodies[put_regions[0]]
                winner = None
                for region in put_regions:
                    if final == contents[region]:
                        winner = region
                        break
                elapsed = (time.perf_counter() - start) * 1000
                results.append((f"Run {i+1}", f"{elapsed:.2f} ms", attempts, winner or "Unknown", "PASS"))
                converged = True
                break
        except Exception:
            pass
    if not converged:
        results.append((f"Run {i+1}", "TIMEOUT", attempts, "N/A", "FAIL"))
    # Cleanup temp files
    for path in files.values():
        os.remove(path)
# ---------- Output ----------
headers = ["Iteration", "Convergence Time", "Attempts", "Winner", "Status"]
print(tabulate(results, headers=headers, tablefmt="grid"))

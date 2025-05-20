import boto3
import requests
import uuid
import time
import os
import hashlib
from threading import Thread
from requests_aws4auth import AWS4Auth
from tabulate import tabulate
print("Concurrent PUTs to the Same Object from Two Regions")
# ---------- CONFIG ----------
endpoint = "https://t3.storage.dev"
bucket = os.getenv("BUCKET", "tigris-consistency-test-bucket")
regions = ["sjc", "fra"]
poll_interval = 1.0
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
# ---------- Utility ----------
def put_object(region, url, file_path):
    with open(file_path, "rb") as f:
        try:
            requests.put(url, data=f, auth=auth, headers={
                "X-Tigris-Regions": region,
            })
        except Exception as e:
            print(f"Error uploading {file_path} to {region}: {e}")
# ---------- Results ----------
results = []
for i in range(iterations):
    print("Iterations:", i + 1)
    object_key = f"simultaneous-write-test-{uuid.uuid4()}"
    url = f"{endpoint}/{bucket}/{object_key}"
    files = {}
    expected = {}
    # Create 1MB files with unique content
    for region in regions:
        path = f"{region}-{uuid.uuid4()}.bin"
        with open(path, "wb") as f:
            f.write(os.urandom(file_size_bytes))
        with open(path, "rb") as f:
            expected[region] = f.read()
        files[region] = path
    # Simultaneous PUTs from SJC and FRA
    threads = []
    for region in regions:
        t = Thread(target=put_object, args=(region, url, files[region]))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    # Poll both regions until they converge to same content/etag
    etags = {}
    sizes = {}
    contents = {}
    for region in regions:
        r = requests.head(f"{url}", headers={
            "X-Tigris-Regions": region,
        }, auth=auth)
        if r.status_code == 200:
            etags[region] = r.headers.get("ETag", "").strip('"')
            sizes[region] = int(r.headers.get("Content-Length", -1))
    start = time.perf_counter()
    deadline = start + max_poll_seconds
    attempts = 0
    converged = False
    while time.perf_counter() < deadline:
        try:
            responses = {}
            if len(etags) == 2 and etags[regions[0]] == etags[regions[1]]:
                elapsed = (time.perf_counter() - start) * 1000
                for region in regions:
                    r1 = requests.get(f"{url}", headers={
                        "X-Tigris-Regions": region,
                    }, auth=auth)
                    if r1.status_code == 200:
                        contents[region] = r1.content
                winner = None
                for region in regions:
                    if contents[region] == expected[region]:
                        winner = region
                        break
                results.append((f"Run {i+1}", f"{elapsed:.2f} ms", attempts, winner or "Unknown", "PASS"))
                converged = True
                break
        except Exception as e:
            print("Error:", e)
        time.sleep(poll_interval)
        attempts += 1
        for region in regions:
            r = requests.head(f"{url}", headers={
                "X-Tigris-Regions": region,
            }, auth=auth)
            if r.status_code == 200:
                etags[region] = r.headers.get("ETag", "").strip('"')
                sizes[region] = int(r.headers.get("Content-Length", -1))
    if not converged:
        results.append((f"Run {i+1}", "TIMEOUT", attempts, "N/A", "FAIL"))
    for f in files.values():
        os.remove(f)
# ---------- Output ----------
headers = ["Iteration", "Convergence Time", "Attempts", "Winner", "Status"]
print(tabulate(results, headers=headers, tablefmt="grid"))

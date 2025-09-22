# sync_to_s3.py
import boto3, requests, hashlib, io, json, re
from urllib.parse import urljoin

S3_BUCKET = "rearc-quest"
S3_PREFIX = "bls/data/"
BLS_BASE = "https://download.bls.gov/pub/time.series/pr/"

s3 = boto3.client("s3")
headers = {"User-Agent": "Maqsud Ilteja (maqsud2097@gmail.com) / bls-data-sync"}

def sha256_bytes(b):
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def get_remote_file(url):
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.content

def s3_head_object(key):
    try:
        return s3.head_object(Bucket=S3_BUCKET, Key=key)
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ['404','NotFound']:
            return None
        raise

def upload_bytes_to_s3(key, bts, sha256):
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=bts,
                  Metadata={"sha256": sha256, "source": "bls-data"})
    print("Uploaded", key)

def get_remote_urls():
    # Scrape the BLS directory index and return full file URLs under the PR dataset
    resp = requests.get(BLS_BASE, headers=headers)
    resp.raise_for_status()
    html = resp.text

    hrefs = re.findall(r'href="([^"]+)"', html)
    files = []
    for name in hrefs:
        # Keep files starting with 'pr.' and skip directories
        if name.startswith('pr.') and not name.endswith('/'):
            files.append(urljoin(BLS_BASE, name))

    return sorted(set(files))

def sync_urls(urls):
    remote_keys = set()
    for url in urls:
        filename = url.split("/")[-1]
        key = S3_PREFIX + filename
        remote_keys.add(key)

        content = get_remote_file(url)
        sha = sha256_bytes(content)

        head = s3_head_object(key)
        s3_sha = None
        if head and 'Metadata' in head:
            s3_sha = head['Metadata'].get('sha256')

        if s3_sha != sha:
            upload_bytes_to_s3(key, content, sha)
        else:
            print("Skipping (unchanged):", key)

    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    if 'Contents' in resp:
        for obj in resp['Contents']:
            if obj['Key'] not in remote_keys:
                print("Deleting stale object:", obj['Key'])
                s3.delete_object(Bucket=S3_BUCKET, Key=obj['Key'])

if __name__ == "__main__":
    urls = get_remote_urls()
    print(f"Discovered {len(urls)} files under BLS PR dataset")
    sync_urls(urls)

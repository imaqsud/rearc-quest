import boto3, requests, hashlib, re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

S3_BUCKET = "rearcio-quest"
S3_PREFIX = "bls/data/"
BLS_BASE = "https://download.bls.gov/pub/time.series/pr/"

s3 = boto3.client("s3")
headers = {"User-Agent": "Maqsud Ilteja (maqsud.ilteja86@gmail.com) / bls-data-sync"}

def sha256_bytes(content):
    h = hashlib.sha256()
    h.update(content)
    return h.hexdigest()

def get_remote_file(url):
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.content

def get_s3_object_metadata(key):
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
    """Scrape the BLS directory index and return all file URLs under the PR dataset"""
    resp = requests.get(BLS_BASE, headers=headers)
    resp.raise_for_status()
    html = resp.text

    urls = []
    try:
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            name = href.split('/')[-1]
            if name.startswith('pr.'):
                urls.append(urljoin(BLS_BASE, href))
    except Exception:
        raise Exception(f"Error fetching BLS urls")

    return sorted(set(urls))

def sync_remote_files(urls):
    """Sync remote files to S3"""
    remote_files = set()
    for url in urls:
        filename = url.split("/")[-1]
        file = S3_PREFIX + filename
        remote_files.add(file)

        content = get_remote_file(url)
        sha = sha256_bytes(content)

        head = get_s3_object_metadata(file)
        s3_sha = None
        if head and 'Metadata' in head:
            s3_sha = head['Metadata'].get('sha256')

        if s3_sha != sha:
            upload_bytes_to_s3(file, content, sha)
        else:
            print("Skipping (unchanged):", file)

    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    if 'Contents' in resp:
        for obj in resp['Contents']:
            if obj['Key'] not in remote_files:
                print("Deleting stale object:", obj['Key'])
                s3.delete_object(Bucket=S3_BUCKET, Key=obj['Key'])

if __name__ == "__main__":
    urls = get_remote_urls()
    print(f"Discovered {len(urls)} files under BLS PR dataset")
    sync_remote_files(urls)

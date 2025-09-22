import json
import boto3
import requests
import hashlib
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def handler(event, context):
    """Lambda function to ingest data from BLS and population APIs"""
    
    s3_client = boto3.client('s3')
    sqs_client = boto3.client('sqs')
    
    bucket_name = "rearcio-quest-bucket"
    
    # Get queue URL by listing queues and finding the ingest queue
    response = sqs_client.list_queues()
    queue_url = None
    for url in response.get('QueueUrls', []):
        if 'IngestQueue' in url:
            queue_url = url
            break
    
    if not queue_url:
        raise Exception("IngestQueue not found")
    
    try:
        # Fetch and sync BLS data
        bls_files_synced = sync_bls_data(s3_client, bucket_name)
        
        # Fetch and upload population data
        population_data_uploaded = fetch_population_data(s3_client, bucket_name)
        
        # Send message to SQS when population data is uploaded
        if population_data_uploaded:
            message = {
                'source': 'population_ingest',
                'timestamp': context.aws_request_id,
                'bucket': bucket_name,
                'key': 'population/population_data.json',
                'status': 'success'
            }
            
            sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message)
            )
            print(f"Sent SQS message for population data upload")
        
        return {
            'status_code': 200,
            'body': json.dumps({
                'bls_files_synced': bls_files_synced,
                'population_uploaded': population_data_uploaded
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'status_code': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def sync_bls_data(s3_client, bucket_name):
    """Sync BLS data files to S3"""
    BLS_BASE = "https://download.bls.gov/pub/time.series/pr/"
    S3_PREFIX = "bls/data/"
    headers = {"User-Agent": "Maqsud Ilteja (maqsud2097@gmail.com) / Rearc Quest - bls-data"}
    
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
            return s3_client.head_object(Bucket=bucket_name, Key=key)
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] in ['404','NotFound']:
                return None
            raise

    def upload_bytes_to_s3(key, bts, sha256):
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=bts,
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
        remote_files = set()
        files_uploaded = 0
    
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
                files_uploaded += 1
            else:
                print("Skipping (unchanged):", file)

        resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=S3_PREFIX)
        if 'Contents' in resp:
            for obj in resp['Contents']:
                if obj['Key'] not in remote_files:
                    print("Deleting stale object:", obj['Key'])
                    s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
            
        return files_uploaded
    
    # Execute BLS sync
    urls = get_remote_urls()
    print(f"Discovered {len(urls)} BLS files")
    files_synced = sync_remote_files(urls)
    print(f"Synced {files_synced} BLS files")
    return files_synced

def fetch_population_data(s3_client, bucket_name):
    """Fetch population data and upload to S3"""
    API_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
    S3_KEY = "population/population_data.json"
    headers = {"User-Agent": "Maqsud Ilteja (maqsud@example.com) / population-data-api-fetch"}
    
    try:
        resp = requests.get(API_URL, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        
        s3_client.put_object(
            Bucket=bucket_name, 
            Key=S3_KEY, 
            Body=json.dumps(data), 
            ContentType="application/json"
        )
        print(f"Saved population data to s3://{bucket_name}/{S3_KEY}")
        return True
    except Exception as e:
        print(f"Error uploading population data: {str(e)}")
        return False
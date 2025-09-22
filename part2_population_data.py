import requests
import boto3
import json

API_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
S3_BUCKET = "rearcio-quest"
S3_KEY = "population/population_data.json"

headers = {"User-Agent": "Maqsud Ilteja (maqsud.ilteja86@gmail.com) / population-data"}
s3 = boto3.client("s3")

resp = requests.get(API_URL, headers=headers)
resp.raise_for_status()
data = resp.json()

s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=json.dumps(data), ContentType="application/json")
print("Saved to s3://{}/{}".format(S3_BUCKET, S3_KEY))

# Rearc Quest - Data Analytics & Pipeline

## Architecture Overview

This project implements a serverless data pipeline that:
1. **Ingests** data from external APIs (BLS and Population)
2. **Stores** data in AWS S3
3. **Processes** data using AWS Lambda functions
4. **Analyzes** data to generate insights and statistics
5. **Orchestrates** the entire pipeline using AWS CDK

## Technologies & Tools

### Technologies
- **Python** - Programming language
- **AWS CDK** - Infrastructure as Code framework
- **AWS Lambda** - Serverless compute for data processing
- **AWS S3** - Object storage
- **AWS SQS** - Message queuing for event-driven architecture
- **AWS EventBridge** - Event scheduling
- **AWS CloudWatch** - Logging and monitoring

### Data Processing Libraries
- **pandas** - Data manipulation and analysis
- **boto3** - AWS SDK for Python
- **requests** - HTTP library for API calls
- **BeautifulSoup4** - Web scraping
- **hashlib** - Hashing for data integrity

## Code details and How to Run 

### Activate virtualenv: `source .venv/bin/activate`

### Install dependencies: `pip install -r requirements.txt`

### Part1 code: `part1_bls_data.py`
- Scrapes the Bureau of Labor Statistics (BLS) directory to discover all PR dataset files
- Implements intelligent sync using SHA256 checksums to avoid re-downloading unchanged files
- Uploads data to S3 with metadata tracking (source, checksum)
- Cleans up stale files that no longer exist in the remote source

#### Run part 1: `python part1_bls_data.py`

#### Link to data in S3
Population data: 
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/population/population_data.json

BLS data:
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.class
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.contacts
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.data.0.Current
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.data.1.AllData
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.duration
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.footnote
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.measure
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.period
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.seasonal
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.sector
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.series
- https://rearcio-quest.s3.ap-south-1.amazonaws.com/bls/data/pr.txt

### Part2 code: `part2_population_data.py`
- Fetches US population data from DataUSA API
- Uploads JSON data directly to S3 for storage

#### Run part 2: python `part2_population_data.py`

### Part3 code (.ipynb): `part3_data_analysis.ipynb`
- Loads BLS and population data from S3
- Performs data cleaning (strips whitespace, standardizes column names)
- Calculates population statistics for 2013-2018 period (mean and std dev)
- Identifies the best performing year for each BLS series by summing quarterly values
- Generates specific report for series `PRS30006032` Q01 data with corresponding population

#### Run part 3: Execute the code directly from the IDE or jupyter notebook

### Part4: Data Pipeline using AWS CDK

#### AWS CDK app: `app.py`
- Entry point of AWS CDK app

#### Stack code: `rearcio_iac/rearci_iac_stack.py`
- Defines the complete AWS infrastructure stack
- Configures Lambda functions, S3 buckets, SQS queues, and EventBridge rules
- Sets up IAM roles and permissions for secure resource access
- Creates S3 bucket for data storage with proper configuration
- Sets up Lambda functions with appropriate runtime and memory settings
- Configures SQS queues for event-driven processing
- Establishes EventBridge rules for scheduled data ingestion
- Manages IAM policies for cross-service communication

#### Lambda code

##### Data Ingestion lambda code: `lambda/ingest/ingest.py`
- Combines BLS and population data fetching into a single Lambda function
- Implements the BLS data sync in serverless environment
- Fetches population data from DataUSA API
- Sends SQS event to trigger downstream analytics processing
- Scheduled execution via EventBridge

##### Analytics lambda code: `lambda/analytics/analytics.py`
- Triggered by SQS event from the ingestion Lambda
- Processes BLS and population data
- Performs the same analysis as Part3 but in serverless environment
- Calculates and generates insights and reports from part 3
- Optimized for Lambda's execution constraints and memory limits

#### Run part 4 (data pipeline): `cd rearci-iac` -> Deploy to AWS - `cdk deploy` -> Trigger ingest lambda function or wait for the lanbda to run as per daily scheduled time -> Check CloudWatch logs

### Debugging
- Check CloudWatch logs
- Verify S3 object metadata and content

## Author

**Maqsud Ilteja**
- Email: maqsud.ilteja86@gmail.com

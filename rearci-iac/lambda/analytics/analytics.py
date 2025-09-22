import json
import boto3
import csv
import io
from collections import defaultdict

def handler(event, context):
    """Lambda function to process analytics"""
    # Bucket name from CDK stack
    bucket_name = "rearcio-quest-bucket"
    
    s3 = boto3.client('s3')
    
    try:
        # Process SQS event
        for record in event['Records']:
            message_body = json.loads(record['body'])
            print(f"Processing message: {message_body}")
            
            # Check if this is a population data upload event
            if message_body.get('source') == 'population_ingest':
                print("Processing data analytics...")
                perform_data_analysis(s3, bucket_name)
                
        return {
            'status_code': 200,
            'body': json.dumps('Analytics processing completed successfully')
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'status_code': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }


def get_bls_data_from_csv_from_s3(s3_client, bucket, key):
    """Load BLS data from S3 CSV"""
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    b = obj['Body'].read()
    try:
        csv_data = []
        csv_reader = csv.DictReader(io.StringIO(b.decode('utf-8')), delimiter='\t')
        for row in csv_reader:
            csv_data.append(row)
        return csv_data
    except Exception:
        raise ValueError("Exception while reading BLS CSV data")

def clean_data(data):
    """Clean data by stripping column names and string values"""
    if not data:
        return data
    
    # Clean column names in the first row and apply to all rows
    first_row = data[0]
    column_mapping = {}
    for key in list(first_row.keys()):
        clean_key = key.strip()
        if clean_key != key:
            column_mapping[key] = clean_key
    
    # Apply column name changes to all rows
    for row in data:
        for old_key, new_key in column_mapping.items():
            if old_key in row:
                row[new_key] = row.pop(old_key)
        
        # Clean string values
        for key, value in row.items():
            if isinstance(value, str):
                row[key] = value.strip()
    
    return data

def get_population_data_from_json_from_s3(s3_client, bucket, key):
    """Load population data from S3 JSON"""
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    pop_json = json.loads(obj['Body'].read())
    if isinstance(pop_json, dict):
        if "data" in pop_json and isinstance(pop_json["data"], list):
            return pop_json["data"]
        else:
            raise ValueError("Invalid population json data structure")
    else:
        raise ValueError("Invalid population json data structure")

def perform_data_analysis(s3_client, bucket_name):
    """Perform data analysis"""
    # Load data
    pr_data = get_bls_data_from_csv_from_s3(s3_client, bucket_name, "bls/data/pr.data.0.Current")
    pr_data = clean_data(pr_data)
    print(f"BLS data: {len(pr_data)} rows")
    
    pop_data = get_population_data_from_json_from_s3(s3_client, bucket_name, "population/population_data.json")
    pop_data = clean_data(pop_data)
    
    for row in pop_data:
        if 'Year' in row and 'year' not in row:
            row['year'] = row.pop('Year')
        if 'Population' in row and 'population' not in row:
            row['population'] = row.pop('Population')
        if 'Nation ID' in row and 'nation_id' not in row:
            row['nation_id'] = row.pop('Nation ID')
        if 'Nation' in row and 'nation' not in row:
            row['nation'] = row.pop('Nation')
    
    print(f"Population data: {len(pop_data)} rows")
    
    for row in pop_data:
        if 'year' in row:
            row['year'] = int(float(row['year'])) if row['year'] else 0
        if 'population' in row:
            row['population'] = float(row['population']) if row['population'] else 0
    
    # Population mean & std deviation for years 2013 to 2018 (inclusive)
    pop_period_data = []
    for row in pop_data:
        year = row.get('year', 0)
        if isinstance(year, (int, float)) and 2013 <= year <= 2018:
            pop_period_data.append(row)
    
    if pop_period_data:
        pop_values = [row['population'] for row in pop_period_data if 'population' in row]
        if pop_values:
            mean_pop = sum(pop_values) / len(pop_values)
            variance = sum((x - mean_pop) ** 2 for x in pop_values) / len(pop_values)
            std_pop = round(variance ** 0.5, 3)
            print(f"Mean population (2013-2018): {mean_pop}")
            print(f"Std dev population (2013-2018): {std_pop}\n")
    
    for row in pr_data:
        if 'value' in row:
            row['value'] = float(row['value']) if row['value'] else 0
    
    # Group by series_id and year, sum values
    series_year_sums = defaultdict(lambda: defaultdict(float))
    for row in pr_data:
        series_id = row.get('series_id')
        year = row.get('year')
        value = row.get('value')
        if series_id and year and value is not None:
            series_year_sums[series_id][year] += value
    
    # Find best year for each series_id
    best_years = []
    for series_id, year_sums in series_year_sums.items():
        if year_sums:
            best_year = max(year_sums.items(), key=lambda x: x[1])
            best_years.append({
                'series_id': series_id,
                'year': best_year[0],
                'summed_value': round(best_year[1], 3)
            })
    
    print(f"Best year for each series id\n {best_years}\n")
    
    # Report for series_id = PRS30006032 and period = Q01, with population for that year
    q1_data = [row for row in pr_data if row.get('series_id') == 'PRS30006032' and row.get('period') == 'Q01']
    
    for row in q1_data:
        if 'year' in row:
            row['year'] = int(float(row['year'])) if row['year'] else 0
    
    # Join with population data
    result = []
    for q1_row in q1_data:
        year = q1_row.get('year')
        population = 0
        for pop_row in pop_data:
            if pop_row.get('year') == year:
                population = pop_row.get('population', 0)
                break
        
        result_row = q1_row.copy()
        result_row['population'] = population
        result.append(result_row)
    
    print(f"Value for series_id = PRS30006032 and period = Q01 and the population for that given year\n{result}")

import requests
import boto3
import json
from datetime import datetime
import schedule
import time


league_codes = ['PL', 'PD', 'BL1', 'SA', 'FL1'] 

api_url_template = "https://api.football-data.org/v4/competitions/{league}/matches"
headers = {
    'X-Auth-Token': ''
}

s3 = boto3.client('s3')
bucket_name = 'football-data-pipeline-rohan'

def fetch_and_store_league_data(league_code):
    api_url = api_url_template.format(league=league_code)
    response = requests.get(api_url, headers=headers)
    data = response.json()
    file_name = f"{league_code}_matches_{datetime.today().strftime('%Y%m%d')}.json"
    try:
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(data))
        print(f"Data stored as {file_name} in S3.")
    except Exception as e:
        print("Failed to store data in S3:", str(e))

def scheduled_task():
    for league_code in league_codes:
        print(f"Fetching data for {league_code} at {datetime.now()}")
        fetch_and_store_league_data(league_code)
    print("Data fetching completed.")

schedule.every().sunday.at("02:39").do(scheduled_task)


if __name__ == '__main__':
   print("Scheduler started.")
   while True:
        schedule.run_pending()
        time.sleep(1)
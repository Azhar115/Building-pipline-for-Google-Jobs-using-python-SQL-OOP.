import api
import config
from datetime import datetime, timedelta
import json
import requests
if __name__ == "__main__":
    # Initialize the API caller
    api_caller = api.Api_Call()
    # Get the current date and time
    now = datetime.now()
    
    start_today = datetime(now.year, now.month, now.day)  # Start of today in UTC
    today_date = start_today.strftime('%Y-%m-%d')
    # Format the timestamp
    
    # Define the API endpoint and parameters
    now = datetime.now()
    twelve_hours_ago = now - timedelta(hours=12)
    twelve_hours_ago = twelve_hours_ago.strftime('%Y-%m-%dT%H:%M:%S')

    # Construct the URL with the necessary parameters
    api_url = "https://serpapi.com/search.json?engine=google_jobs&q=barista+new+york&hl=en"
    
    # Parameters for the API call
    params = {
        
        "api_key": "01c6699ea78143a7d0e4018e14c129ae85fb981846bf3a07755c76e5f5a573b4" 
    }
   
    api_caller.set_api_params(api_url, params)
    check = api_caller.all_google_pages_jobs_access_time(today_date, Grain_time= True, is_stg_schema_already_created= False)
    if check == 1:
        print("work done successfully")
    else:
        print("some errors")
   # api_caller.set_api_params(Api, params=params)
   
  



import requests
import json
from datetime import datetime, timedelta

if __name__ == "__main__":
    # Get the current UTC time
    now = datetime.now()  # Use utcnow() to get UTC time

    # Start of today in UTC
    start_today = datetime(now.year, now.month, now.day)
    # Format the dates
    todaydate = start_today.strftime('%Y-%m-%d')
    #twelve_hours_ago_date = twelve_hours_ago.strftime('%Y-%m-%d %H:%M:%S')

    page_result = []
    next_page_token = None

    # Define the API endpoint
    Api = "https://serpapi.com/search.json?"
    

    while True  :
       
      
        print("Fetching data...")

        # Define parameters
        parms = {
            "engine": "google_jobs",
            "q": "barista new york",
            "hl": "en",
            "api_key":  "a86b4f814dea5fb23d0cf2ec06c25b298dd0cbeb24c2704a984ed80c51a380e1",  # Replace with your actual API key
            "chips": f"date_posted:{todaydate}",
            "next_page_token": next_page_token  # Ensure parameter name matches API specification
        }

        try:
            # Make the API request
            response = requests.get(Api, params=parms)
            response.raise_for_status()  # Raise an error for bad responses

            # Process the JSON response
            response_data = response.json()
            page_result.append(response_data)

            # Check for the next page
            next_page_token = response_data["serpapi_pagination"]["next_page_token"]

            print(f"Next page token: {next_page_token}")

        except requests.RequestException as e:
            print(f"API request failed: {e}")
            break  # Exit loop on request failure
        except KeyError as e:
            print(f"Error processing response: {e}")
            break  # Exit loop on missing expected data

    # Save the response JSON structure to a file
    json_file_path = "jobsss_today.json"
    with open(json_file_path, 'w') as json_file:
        json.dump(page_result, json_file, indent=4)

    print(f"Data saved to {json_file_path}")

import requests
import pandas as pd
import json
import transformation_google_product as automic
import config
from sqlalchemy import text
from datetime import datetime

class Api_Call:
   
    def __init__(self,api=None, params=None ):
        """Set the API URL and parameters."""
        self.__api = api  #globsl attributes initialization
        self.__params = params
        
    def set_api_params(self, api=None, params=None):
        """Set the API URL and parameters."""
        self.__api = api  #globsl attributes initialization
        self.__params = params



    def __get_google_product_data(self, api_url, params, current_date, server_connection):
        now = datetime.now()
        current_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

        # Log file attributes
        page_jobs_error = None
        page_jobs_count = None  # Remove the comma here
        response_status_code = 200
        is_job_result = True
        google_page_created_at = None
        google_page_processed_at = None
        google_page_result_total_timetaken = None
        google_page_jobs_url = None

        try:
            if api_url is not None:
                response = requests.get(api_url, params=params)
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

                response_status_code = response.status_code
                response_dict = response.json()

                # Check for errors in the response
                page_jobs_error = response_dict.get("error")
                if "jobs_results" in response_dict:
                    is_job_result = True
                    page_jobs_count = len(response_dict["jobs_results"])

                # Extract metadata
                google_page_jobs_url = response_dict["search_metadata"]["google_jobs_url"]
                google_page_created_at = response_dict["search_metadata"]["created_at"]
                google_page_processed_at = response_dict["search_metadata"]["processed_at"]
                google_page_result_total_timetaken = response_dict["search_metadata"]["total_time_taken"]
            
            return response, response_status_code
        except requests.RequestException as e:
            print(f"API request failed: {e}")
            response_status_code = e.response.status_code if e.response else 1
            return None, response_status_code  # Return status code if available
        finally:
            # Log the data to the database
            if api_url is None:
                api_url = " No api to call "
            query = """
                INSERT INTO log_google_jobs_page (
                    page_jobs_error,
                    page_jobs_count,
                    response_status_code,
                    api_url,
                    is_job_result,
                    page_date,
                    page_timestamp,
                    google_page_created_at,
                    google_page_processed_at,
                    google_page_result_total_timetaken,
                    google_page_jobs_url
                ) VALUES (
                    :page_jobs_error,
                    :page_jobs_count,
                    :response_status_code,
                    :api_url,
                    :is_job_result,
                    :page_date,
                    :page_timestamp,
                    :google_page_created_at,
                    :google_page_processed_at,
                    :google_page_result_total_timetaken,
                    :google_page_jobs_url
                );
            """

            # Prepare data for insertion
            dummy_data = {
                'page_jobs_error': page_jobs_error,
                'page_jobs_count': page_jobs_count,
                'response_status_code': response_status_code,
                'api_url': api_url,
                'is_job_result': is_job_result,
                'page_date': current_date,
                'page_timestamp': current_timestamp,
                'google_page_created_at': google_page_created_at,
                'google_page_processed_at': google_page_processed_at,
                'google_page_result_total_timetaken': google_page_result_total_timetaken,
                'google_page_jobs_url': google_page_jobs_url
            }

            # Format the query with actual values
            formatted_query = query
            for key, value in dummy_data.items():
                if isinstance(value, str):
                    formatted_value = f"'{value}'"
                elif isinstance(value, (datetime,)):
                    formatted_value = f"'{value.isoformat()}'"
                elif value is None:
                    formatted_value = 'NULL'
                else:
                    formatted_value = str(value)

                formatted_query = formatted_query.replace(f":{key}", formatted_value)

            try:
                # Execute the query with formatted values
                result = server_connection.execute(text(formatted_query))
                if result.rowcount > 0:
                    server_connection.commit()
                    print("Insert successful")
                else:
                    print("Insert failed or no rows affected")
            except Exception as e:
                print(f"Insert error: {e}\nQuery: {formatted_query}\nValues: {dummy_data}")



        
    def save_api_result_json(self,file_name='google_jobs.json'):
        """Save the response JSON structure to a file."""
        response = self.__get_google_product_data(self.__api,self.__params)
        json_file_path = file_name
        with open(json_file_path, 'w') as json_file:
            json.dump(response, json_file, indent=4)

    def __read_json_file(self, json_file_path):
        """Read a JSON file and normalize any dictionary values into DataFrames."""
        with open(json_file_path, 'r') as json_file:
            dic_js = json.load(json_file)  # Load JSON into a Python dictionary

        normalized_dfs = []
        for key, value in dic_js.items():
            if isinstance(value, dict):
                normalized_df = pd.json_normalize(value)  # Normalize the dictionary to a DataFrame
                normalized_dfs.append(normalized_df)
        
        # Optionally concatenate all normalized DataFrames
        if normalized_dfs:
            combined_df = pd.concat(normalized_dfs, ignore_index=True)
            print(combined_df)
        else:
            print("No dictionaries found to normalize.")

    def __json_to_csv(self, json_data):
        """Convert JSON data to a DataFrame and save it as a CSV file."""
        df = pd.json_normalize(json_data)  # Handle nested data
        df.to_csv("google_products.csv", index=False)  # Save DataFrame to CSV
        print("Data saved to google_products.csv")

    def access_records_google_jobs(self, next_page=None):
        """Prepare the API request parameters for Google Jobs."""
        source_api = self.__api
        params = self.__params
        params["next_page_token"] = next_page
        
        return source_api, params
    
    def __making_source_data_atomic_1nf(self, current_date,server_connection, next_page_google_job_api_url):
        """
        Function Working : this function request from google jobs api with next_google_job_page_token to get that page jobs data in json form
        
        __parameters:
        next_google_job_page_result : takes as input next_page google jobs json data fetching token

        __return:
        automic_google_jobs_page_result_df : 1st Form dataframe
        next_page_token, returns the next page token to request google jobs data
        response_status 
        """
        #adding next page pramater in api
       # source_api, params = self.__access_records_google_jobs(next_google_job_page_token) # sending next page as none because it is first time call, initial

        # requesting api for data getting in json formate 
        google_jobs_page_result, response_status_code = self.__get_google_product_data(next_page_google_job_api_url, self.__params, current_date, server_connection)
        
        #checking if json file is return or responce status is not 200
        if google_jobs_page_result is None or response_status_code != 200:
            return google_jobs_page_result, None, response_status_code

        # Normalize the JSON dataframe response 
        json_google_page_norm_df = pd.json_normalize(google_jobs_page_result)
        # Replace dots with underscores in column names
        #make automic attributes, conversion into 1NF form the data frame
        json_google_page_norm_df.columns = json_google_page_norm_df.columns.str.replace('.', '_')
        automic_google_jobs_page_result_df = automic.flattenDataframe(json_google_page_norm_df)
        automic_google_jobs_page_result_df.columns = automic_google_jobs_page_result_df.columns.str.replace('.', '_')

       
        
        #check if next page exist or not, if not returns 
        try:
            next_page_api_url = google_jobs_page_result["serpapi_pagination"]["next"]   # key error generates
            #not key in json     
        except KeyError:
                # means no more pages to scrape data from
                print("No next page")
                next_page_api_url = None

        return automic_google_jobs_page_result_df, next_page_api_url, response_status_code  

    def __api_pagination_hit_google_jobs_page(self, current_date, server_connection, is_stg_schema_created= True, next_page_api_url= None):
        """ is_stg_schema_created, it will creat stg schema for google jobs product by its automic data frame and storing
        first time transformed sutomic data in it.
        otherwise, it will call stg_storeprocedure for data append in stg table in transformed form

        __parameters:
        server_connection : db_connected point cursor
        is_stg_schema_created : checking either to creat schema for stg or directly append data in stg schema
        next_page_token : page from which data to be requested by api and append in stg in transformed formate

        __return:
        google_jobs_next_page_token : 
        """

        #if stg table not created, first creat and store the normalized data frame in stg
        # Save the DataFrame to PostgreSQL
        automic_google_jobs_page_result_df, next_page_api_url, response_status_code = self.__making_source_data_atomic_1nf(
                    current_date,
                    server_connection,
                    next_page_api_url
                    )
        #checking 
        print("page data is converted into 1nf")

        if response_status_code == 200:
            if not is_stg_schema_created  :
                table_name = 'google_jobs_page_stg'  # Specify the name of the stg table
                #initial load needs schema to be created for storing data
                    
                # Use if_exists='replace' to replace the table if it exists
                try:
                    automic_google_jobs_page_result_df.to_sql(table_name, server_connection , if_exists='replace', index=False)
                    print(f"DataFrame saved to {table_name} in PostgreSQL database.")
                    # Define the PL/pgSQL block as a string
                    query = """
                    DO $$
                    DECLARE
                           is_table_created BOOLEAN;
                    BEGIN
                        CALL creating_production_table_initial_load(is_table_created);
                        RAISE NOTICE 'Table created: %', is_table_created;
                    END $$;
                    """

                    # Execute the PL/pgSQL block
                    server_connection.execute(text(query))

                    query_check = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'production_google_jobs_pages');"

                    # Check if the table was created
                    check_result = server_connection.execute(text(query_check))
                    table_exists = check_result.scalar()
                    if table_exists:
                        print("stg table created successfully and initially loaded data")
                except Exception as e:
                        print("make a table for keeping status log")
                        print("Table was not created.")        
            else :
                # incrementally loading all page jobs data into stg of current date 
                table_name = "google_jobs_page_stg"
                automic_google_jobs_page_result_df.to_sql(table_name, server_connection , if_exists='append', index=False)
                print(f"google page appended   to {table_name} incrementally in PostgreSQL database.")       
        else:
            pass

        return next_page_api_url  # Return result for pagination handling

 

    def all_google_pages_jobs_access_time(self, current_date, Grain_time=False, is_stg_schema_already_created = True):
        """(pagination Fetch all job listings of one page at a time with optional time tracking."""

        #grain time is true start data digestion from google jobs pages into staging 
        if Grain_time :
            try:
                # make connection with server for staging data storage
                connection_engine= config.config_postgre_for_pandas_frame()
                #automatically connection close after fetching all pages
                with connection_engine.connect() as server_connection:
                    #pagination of google pages on grain time
                    google_jobs_next_page_api_url = self.__api  # Initialize next page token
                    
                    while True:
                     
                        try:
                            print("page hit start")
                            google_jobs_next_page_api_url = self.__api_pagination_hit_google_jobs_page(current_date,
                                server_connection,
                                is_stg_schema_created = is_stg_schema_already_created,
                                next_page_api_url= google_jobs_next_page_api_url
                                )
                            is_stg_schema_already_created = True
                            if google_jobs_next_page_api_url is None:
                                break  # no next page stop and exit loop of pagination
                            print("page hit end ")
                        except Exception as e:
                            #means no more pages to scrape data from
                            print(f"No next page or error {e}")
                            break   
                    #storing successful data extracting from all pages from api google jobs into stg into log file and status log file 

                    #all pages jobs data is loaded into stg table of current date, now append it into production table by store procedure
                    query =  """DO $$
                    DECLARE
                        is_table_created BOOLEAN;
                    BEGIN
                        CALL insertion_from_stg_to_production_table(is_table_created);
                        RAISE NOTICE 'Table data inserted: %', is_table_created;
                    END $$;
                    """
                    print("today all pages data loading starts from stg to productions")
                    server_connection.execute(text(query))
                    print("ends loading from stg to productions")
                    

                return 1  # Pagination completed successfully in grain time
            except Exception as e:
               print(f"An error occurred: {e}")
               #storing error in log file and status log file
        else:
            return -1     #error in pagination no data requested from api at grain time

            #storing error in log file and status log file
        
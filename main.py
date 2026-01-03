from google.cloud import bigquery
from pandas import json_normalize
import pandas as pd
import requests
import base64
import json
import time
import os


# Set your Google Cloud credentials path
PROJECT_ID = os.getenv('GCP_PROJECT')
# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)


def write_bq(data_df, table_id):
    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )

        job = client.load_table_from_dataframe(
            data_df, table_id, job_config=job_config
        )

        print(job.result())
        return job.result()

    except Exception as e:
        print(f"An error occurred: {e}")
        return e

def table_exists(table_id):
    client = bigquery.Client(project=table_id.split(".")[0])
    dataset_ref = client.dataset(table_id.split(".")[1])
    table_ref = dataset_ref.table(table_id.split(".")[2])

    try:
        client.get_table(table_ref)
        return True
    except Exception as e:
        # Table does not exist or other error occurred
        return False


def fetch_data(api_url, query_params, bearer_token):
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json',
    }

    response = requests.get(api_url, params=query_params, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None
 

def hello_pubsub(event, context):
    
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)

    json_data = json.loads(pubsub_message) 
    print(f"Received JSON data: {json_data}")
    table_id = json_data["dest_table"]

    query_string = f"""
        select 
        distinct app_url, 
        CONCAT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', MAX(DATE(Created_Date))),'Z') max_date
        from 
        `{table_id}`
        where 
        app_url = '{json_data["bubble_app_url"]}'
        group by 
        1
        """
    
    max_date = None

    if table_exists(table_id):
        print(f'Dest. Table {table_id} exists.')
        assets_df = (client.query(query_string).result().to_dataframe())
        if assets_df.empty:
            # Full/First Load
            flag = 0 
            print("(FL.) Dest. Table does not have app data!")
        else:
            # Incremental Load
            flag = 1
            max_date = assets_df["max_date"].iloc[0]
            print("(Incr.) Dest. Table Max. Date:",max_date)
    else:
        print(f'Dest. Table {table_id} does not exist. [Creating Table in Full Load Mode]')
        flag = 0
    

    data_df = pd.DataFrame()

    match flag:
            case 0:
                print("First/Full Load")
                init_query_params = {
                    'additional_sort_fields': '[{"sort_field": "Created Date", "descending": "true"}]'
                }

                # Fetch data from the API
                init_api_response = fetch_data(json_data["bubble_app_url"], init_query_params, json_data["bubble_token"])

                if init_api_response:
                    # Convert the API response to a DataFrame and store init run data
                    data_df = json_normalize(init_api_response['response']['results'])   
                else:
                    print("Bubble API - Fail")
                
                rows_left = init_api_response['response']['remaining']
                curr_rows = init_api_response['response']['count']
                cursor = curr_rows

                print("Init Run API call, Rows Left:", rows_left)
                
                while cursor < (rows_left + curr_rows):
                    print("(FL.) Cursor Pg. :",cursor," Curr Len:",len(data_df))
                    curr_query_params = {
                    'additional_sort_fields': '[{"sort_field": "Created Date", "descending": "true"}]',
                    'cursor' : cursor
                    # ,'limit': 99
                    }

                    curr_api_response = fetch_data(json_data["bubble_app_url"], curr_query_params, json_data["bubble_token"])
                    
                    if curr_api_response:
                        curr_data_df = json_normalize(curr_api_response['response']['results'])
                        data_df = pd.concat([data_df, curr_data_df], ignore_index=True)
                    else:
                        print("(Full Load) Bubble API Failed at: ", cursor)

                    cursor += 100
      
            case 1:
                print("Incremental Load")
               
                init_query_params = {
                    'constraints': f'[{{"key": "Created Date", "constraint_type": "greater than", "value": "{max_date}"}}]',
                    'additional_sort_fields': '[{"sort_field": "Created Date", "descending": "true"}]'
                }

                # Fetch data from the API
                init_api_response = fetch_data(json_data["bubble_app_url"], init_query_params, json_data["bubble_token"])

                if init_api_response:
                    # Convert the API response to a DataFrame and store init run data
                    data_df = json_normalize(init_api_response['response']['results'])   
                else:
                    print("Bubble API - Fail")
                
                rows_left = init_api_response['response']['remaining']
                curr_rows = init_api_response['response']['count']
                cursor = curr_rows

                while cursor < (rows_left + curr_rows):
                    print("(Incr.) Cursor Pg. :",cursor," Curr Len:",len(data_df))
                    curr_query_params = {
                    'constraints': f'[{{"key": "Created Date", "constraint_type": "greater than", "value": "{max_date}"}}]',
                    'additional_sort_fields': '[{"sort_field": "Created Date", "descending": "true"}]',
                    'cursor' : cursor
                    # ,'limit': 99
                    }

                    curr_api_response = fetch_data(json_data["bubble_app_url"], curr_query_params, json_data["bubble_token"])
                    
                    if curr_api_response:
                        curr_data_df = json_normalize(curr_api_response['response']['results'])
                        data_df = pd.concat([data_df, curr_data_df], ignore_index=True)
                    else:
                        print("(Incr.) Bubble API Failed at: ", cursor)

                    cursor += 100

            case _:
                print("Unknown Flag!")

    
    if not data_df.empty:
        # Print or manipulate the DataFrame as needed
        data_df['sync_time'] = time.time()
        data_df['client_name'] = json_data["client_name"]
        data_df['app_url'] = json_data["bubble_app_url"]
        data_df.columns = data_df.columns.str.replace('.', '_')
        data_df.columns = data_df.columns.str.replace(' ', '_')

        print("DataFrame Head:",data_df.head())
        print("DataFrame Length:",len(data_df))
        
        write_bq(data_df, table_id)
    else:
        print("DataFrame Empty!")

    print("Exiting Function!")
    
    



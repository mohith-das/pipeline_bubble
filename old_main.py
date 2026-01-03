from google.cloud import bigquery
# from google.cloud import pubsub_v1
import pandas as pd
from pandas import json_normalize
import requests
import base64
import json
import time
import os


# Set your Google Cloud credentials path
PROJECT_ID = os.getenv('GCP_PROJECT')
# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)
# Define your BigQuery dataset and table
table_id = 'solutionsdw.User_Behaviour_Data.bubbleApp_ubt_datatable'




def write_bq(data_df, table_id):
    try:
        # dataset_ref = client.dataset(dataset_id)
        # table_ref = dataset_ref.table(table_id)
        # table = client.get_table(table_ref)

        job_config = bigquery.LoadJobConfig(
            # createDisposition="CREATE_IF_NEEDED",
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
    

#     api_url = "https://analytics.amzatlas.com/version-test/api/1.1/obj/usertracking"
    # Specify your query parameters and bearer token
    query_params = {
        # 'constraints': '[{"key": "_id", "constraint_type": "equals", "value": "1697204931756x480013709809811460"}]',
        'additional_sort_fields': '[{"sort_field": "Created Date", "descending": "true"}]'
    }
#     bearer_token = os.getenv('BUBBLE_API_TOKEN', 'replace_bubble_api_token')
    
    
    # Fetch data from the API
    api_response = fetch_data(json_data["bubble_app_url"], query_params, json_data["bubble_token"])

    if api_response:
        # Convert the API response to a DataFrame
        data_df = json_normalize(api_response['response']['results'])
        # Print or manipulate the DataFrame as needed
        data_df['sync_time'] = time.time()
        data_df['client_name'] = json_data["client_name"]
        data_df['app_url'] = json_data["bubble_app_url"]

        # data_df['Modified Date'] = pd.to_datetime(data_df['Modified Date'])
        # data_df['Created Date'] = pd.to_datetime(data_df['Created Date'])
        # data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
        # data_df['viewCount'] = pd.to_numeric(data_df['viewCount'])

        # columns_to_convert = ['Created By', 'email', 'dashboardName', 'sessionID', 'clickType', '_id']
        # data_df[columns_to_convert] = data_df[columns_to_convert].astype(str)
        data_df.columns = data_df.columns.str.replace(' ', '_')

        print(data_df.head())
        write_bq(data_df, table_id)

    else:
        print("Bubble API - Fail")


from global_common import *
import json
import os
import snowflake.connector
import boto3
from botocore.exceptions import ClientError
import base64
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.authentication_context import AuthenticationContext
import pandas as pd
from snowflake.connector import DictCursor

# Initialize S3 client
s3_client = boto3.client('s3')

def download_sharepoint_file(file_path, file_name, client_id, client_secret):
    """
    Download a file from SharePoint and save it locally.
    """
    site_url = "FILE URL"
    context_auth = AuthenticationContext(url=site_url)
    context_auth.acquire_token_for_app(client_id=client_id, client_secret=client_secret)
    
    ctx = ClientContext(site_url, context_auth)
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
    print(f"Site title: {web.properties['Title']}")
    
    # Define SharePoint file URL
    FILE_URL = f'URL HERE/{file_name}'
    response = File.open_binary(ctx, FILE_URL)
    
    with open(file_path, "wb") as local_file:
        local_file.write(response.content)
    
    print(f"Your file is downloaded to: {file_path}")
    return file_path

def load_file_to_s3(file_path, s3_bucket_name, s3_path):
    """
    Upload the file to S3, converting it to CSV if necessary.
    """
    try:
        if file_path.endswith('.xlsx'):
            new_s3_path = s3_path.replace('.xlsx', '.csv')
            new_file_path = file_path.replace('.xlsx', '.csv')
            csv_df = pd.read_excel(file_path)
            csv_df.to_csv(new_file_path, index=False)
        else:
            new_s3_path = s3_path
            new_file_path = file_path
            
        s3_client.upload_file(new_file_path, s3_bucket_name, new_s3_path)
        print(f"Uploaded {new_file_path} to bucket {s3_bucket_name} at path: {new_s3_path}")
        return new_s3_path
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        raise e

def copy_file_to_snowflake(cur, database_name, source_s3_bucket, s3_file_path):
    """
    Copy the file from S3 into Snowflake.
    """
    snowflake_stage = os.environ['SNOWFLAKE_STAGE']
    copy_statement = f'''
    COPY INTO {database_name}.SCHEMA.SHAREPOINT_FILE(COLUMN_1, COLUMN_2, COLUMN_3)
    FROM (
        SELECT 
            t.$1, t.$2, t.$3, 's3://{source_s3_bucket}/' || metadata$filename, metadata$file_row_number
        FROM @"{snowflake_stage}"."{s3_file_path}" 
        FILE_FORMAT = (FORMAT_NAME = 'SHAREPOINT_CSV', PATTERN = '.*{s3_file_path}')
    ) AS t;
    '''
    print(copy_statement)
    cur.execute(copy_statement)

def ingest_data_to_snowflake(s3_bucket, s3_path):
    """
    Ingest data into Snowflake from S3.
    """
    try:
        snowflake_database = os.environ['SNOWFLAKE_DATABASE']
        snowflake_warehouse = os.environ['SNOWFLAKE_WAREHOUSE']
        secrets = get_secret()
        conn = get_snowflake_connection(secrets)
        cur = conn.cursor(DictCursor)
        
        # Create table if not exists and truncate previous data
        cur.execute(f'''
        CREATE TABLE IF NOT EXISTS {snowflake_database}.SCHEMA.SHAREPOINT_FILE (
            COLUMN_1 VARCHAR, COLUMN_2 VARCHAR, COLUMN_3 VARCHAR
        );
        ''')
        cur.execute(f"USE WAREHOUSE {snowflake_warehouse};")
        cur.execute(f"TRUNCATE TABLE {snowflake_database}.SCHEMA.SHAREPOINT_FILE;")
        
        # Copy data from S3 to Snowflake
        copy_file_to_snowflake(cur, snowflake_database, s3_bucket, s3_path)
    except Exception as e:
        print(f"Error ingesting data into Snowflake: {e}")
        raise e

def lambda_handler(event, context):
    """
    AWS Lambda handler function to orchestrate the data ingestion.
    """
    try:
        print("Loading data from SharePoint to Snowflake")
        FILE_NAME = os.environ['SHAREPOINT_FILE_NAME']
        FILE_PATH = f'/tmp/{FILE_NAME}'
        s3_bucket = os.environ['S3_BUCKET']
        s3_file_path = f'sharepoint/{FILE_NAME}'
        
        # Fetch SharePoint credentials
        sharepoint_client_info = get_secret_by_name('MKT-sharepoint-client-SECRET')
        sharepoint_client_id = sharepoint_client_info['client_id']
        sharepoint_client_secret = sharepoint_client_info['client_secret']
        
        # Download file from SharePoint
        download_sharepoint_file(file_path=FILE_PATH, file_name=FILE_NAME, client_id=sharepoint_client_id, client_secret=sharepoint_client_secret)
        
        # Upload the file to S3
        csv_s3_path = load_file_to_s3(file_path=FILE_PATH, s3_bucket_name=s3_bucket, s3_path=s3_file_path)
        
        # Ingest data into Snowflake
        ingest_data_to_snowflake(s3_bucket, csv_s3_path)
        
        return "Data ingestion completed."
    except Exception as e:
        print(f"Error executing Lambda function: {e}")
        raise e

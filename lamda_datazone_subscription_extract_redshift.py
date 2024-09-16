import sys
import configparser
import boto3
import csv
import os
import time
from datetime import datetime
from botocore.exceptions import ClientError
import json


def load_csv_from_s3_to_redshift(s3_bucket, s3_key, redshift_database, redshift_schema, redshift_table, redshift_cluster_id, secret_arn, iam_role):
    """
    Check if the Redshift table exists, truncate it if it does, and load a CSV file from S3 into the Redshift table using the Redshift Data API.

    :param s3_bucket: Name of the S3 bucket
    :param s3_key: Key of the CSV file in the S3 bucket
    :param redshift_database: Name of the Redshift database
    :param redshift_schema: Name of the Redshift schema
    :param redshift_table: Name of the target Redshift table
    :param redshift_cluster_id: ID of the Redshift cluster
    :param redshift_db_user: Redshift database user
    :param iam_role: IAM role ARN with necessary permissions
    """
    try:
        # Create a Redshift Data API client
        redshift_data = boto3.client('redshift-data')

        # Check if the table exists
        check_table_sql = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{redshift_schema}'
            AND table_name = '{redshift_table}'
        );
        """

        response = redshift_data.execute_statement(
            ClusterIdentifier=redshift_cluster_id,
            Database=redshift_database,
            #DbUser=redshift_db_user,
            SecretArn= secret_arn,
            Sql=check_table_sql
        )

        # Wait for the query to complete and get the result
        table_exists = wait_for_statement_completion(redshift_data, response['Id'])

        if table_exists:
            print(f"Table {redshift_schema}.{redshift_table} exists. Truncating...")
            truncate_sql = f"TRUNCATE TABLE {redshift_schema}.{redshift_table};"
            response = redshift_data.execute_statement(
                ClusterIdentifier=redshift_cluster_id,
                Database=redshift_database,
                #DbUser=redshift_db_user,
                SecretArn= secret_arn,
                Sql=truncate_sql
            )
            wait_for_statement_completion(redshift_data, response['Id'])
            print(f"Table {redshift_schema}.{redshift_table} truncated successfully.")
        else:
            print(f"Table {redshift_schema}.{redshift_table} does not exist. Skipping truncate.")

        # Construct the COPY command
        copy_command = f"""
        COPY {redshift_schema}.{redshift_table}
        FROM 's3://{s3_bucket}/{s3_key}'
        IAM_ROLE '{iam_role}'
        CSV
        IGNOREHEADER 1
        """

        # Execute the COPY command using the Redshift Data API
        response = redshift_data.execute_statement(
            ClusterIdentifier=redshift_cluster_id,
            Database=redshift_database,
            #DbUser=redshift_db_user,
            SecretArn= secret_arn,
            Sql=copy_command
        )

        # Wait for the COPY command to complete
        wait_for_statement_completion(redshift_data, response['Id'])
        print(f"Successfully loaded data from s3://{s3_bucket}/{s3_key} into {redshift_schema}.{redshift_table}")
        return 0

    except ClientError as e:
        print(f"Error: {e}")
        return 1

def wait_for_statement_completion(redshift_data, statement_id):
    """
    Wait for a Redshift Data API statement to complete and return the result.
    """
    while True:
        status_response = redshift_data.describe_statement(Id=statement_id)
        status = status_response['Status']

        if status == 'FINISHED':
            if 'Records' in status_response:
                return status_response['Records'][0][0]  # Return the first column of the first row
            return True
        elif status == 'FAILED':
            error = status_response.get('Error', 'Unknown error')
            raise Exception(f"Query failed: {error}")
        elif status == 'ABORTED':
            raise Exception("Query was aborted")
        else:
            time.sleep(2)  # Wait for 2 seconds before checking again

def lambda_handler(event, context):

    # read from aws lambda environment configuration variables throw an error if not found
    try:
        domain_id = os.environ['domain_id']
        project_id = os.environ['project_id']
        bucket_name = os.environ['bucket_name']
        csv_file_path = os.environ['csv_file_path']
        secret_name = os.environ['secret_name']
        redshift_database = os.environ['redshift_database']
        redshift_schema = os.environ['redshift_schema']
        redshift_table = os.environ['redshift_table']
        redshift_iam_role = os.environ['redshift_iam_role']

    except KeyError as e:
        print(f"Error: {e}")

    session = boto3.session.Session()
    region = session.region_name
    client = session.client(
    service_name='secretsmanager',
        region_name=region
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    secret_arn=get_secret_value_response['ARN']
    secret = json.loads(get_secret_value_response['SecretString'])
    print(secret)
    cluster_id = secret['dbClusterIdentifier']
    
    redshift_result = load_csv_from_s3_to_redshift(bucket_name, csv_file_path, redshift_database, redshift_schema, redshift_table, cluster_id, secret_arn, redshift_iam_role)
    if redshift_result == 1:
        print("Error: Failed to load data into Redshift table")
    else:
        print("Data has been loaded into Redshift table successfully.")


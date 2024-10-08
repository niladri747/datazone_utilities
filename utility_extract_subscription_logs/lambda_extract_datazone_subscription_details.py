import sys
import configparser
import boto3
import csv
import os
from datetime import datetime
from botocore.exceptions import ClientError

def keys_exists(element, *keys):
    '''
    Check if *keys (nested) exists in `element` (dict).
    '''
    if not isinstance(element, dict):
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True

def get_datazone_subscription_details_csv(domain_id, project_id, bucket_name, csv_file_path):
    """
    Retrieves the subscription details for data assets in an Amazon DataZone project and writes them to a CSV file.
    
    Args:
        domain_id (str): The ID of the Amazon DataZone domain.
        project_id (str): The ID of the Amazon DataZone project.
        csv_file_path (str): The file path to write the CSV file in S3.
    """
    session = boto3.Session()
    datazone = session.client('datazone')

    # Get the list of projects in the domain using the list_projects API and a paginator
    projects = []
    paginator1 = datazone.get_paginator('list_projects')
    for page in paginator1.paginate(domainIdentifier=domain_id):
        projects.extend(page['items'])
    #print(projects)
    
    # Create a list of dictionaries with only the 'name' and 'id' keys for each project
    keys = ['name', 'id']
    dict2 = []
    for project in projects:
        dict2.append({x:project[x] for x in keys})

    # Find the project ID for the given project name
    for item in dict2:
    # Check if the value of the match_key matches the desired value
        if item['name'] == project_id:
        # Select the value of the select_key
            selected_value = item['id']
            #print(f"Selected value for {project_id}: {selected_value}")    
    project_id = selected_value

    print(f"Processing project ID: {project_id}")    
    #local_csv_file_path = os.path.join(os.getcwd(), 'subscriptions.csv')
    # Set the local CSV file path
    local_csv_file_path = os.path.join('/tmp/', 'subscriptions.csv')

    try:
        subscriptions = []

        # Open the CSV file for writing
        with open(local_csv_file_path, mode='a', newline='') as csv_file:
            fieldnames = ['Datazone Asset Name', 'Asset System Name', 'Subscription ID', 'Subscriber Project', 'Subscription Status', 'Subscription Request ID', 'Created At', 'Created By', 'Updated At', 'Updated By', 'filter_name', 'filter_columns', 'filter_row_expression']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)            
            # Write the header row
            writer.writeheader()

            # Paginate through the list of subscriptions for the project
            paginator = datazone.get_paginator('list_subscriptions')
            for page in paginator.paginate(approverProjectId=project_id, domainIdentifier=domain_id):
                subscriptions.extend(page['items'])


                if not subscriptions:
                    # Write a row with no subscription details
                    writer.writerow({'Asset Name': '', 'Filters': ''})
                else:
                    for subscription in subscriptions:
                        subscription_id = subscription['id']
                        print(f"Processing subscription ID: {subscription_id}")
                        subscription_details = datazone.get_subscription(domainIdentifier=domain_id, identifier=subscription_id)
                        print(subscription_details)
                        asset_id = subscription_details['subscribedListing']['item']['assetListing']['entityId']
                        print(f"Processing asset ID: {asset_id}")
                        asset_details = datazone.get_asset(domainIdentifier=domain_id, identifier=asset_id)
                        asset_system_name = asset_details['externalIdentifier']
                        filters = []
                        filter_name =[]
                        filter_columns = []
                        filter_row_expression = []
                        
                        assetcope_key_check = keys_exists(subscription_details, "subscribedListing","item","assetListing","assetScope")
                        print(assetcope_key_check)
                        
                        if assetcope_key_check is False:
                            filter_name = 'Full Access'
                            filter_columns = []
                            filter_row_expression = []
                            
                        else:
                            asset_scope = subscription_details['subscribedListing']['item']['assetListing']['assetScope']
    
                            # Get the list of filter IDs for the asset
                            filters = asset_scope['filterIds']
                            print(filters)
    
                            
                        for filter in filters:
                            # Get the details of each filter
                            filter_details = datazone.get_asset_filter(assetIdentifier = asset_id, domainIdentifier = domain_id, identifier = filter)
                            filter_name.append(filter_details['name'])
                            if 'effectiveColumnNames' in filter_details:
                                filter_columns = filter_details['effectiveColumnNames']
    
                            if 'effectiveRowFilter' in filter_details:
                                filter_row_expression = filter_details['effectiveRowFilter']
    
                            # Find the subscription request ID for the subscription to find the requester of the subscription
                            subscription_request_id = subscription_details['subscriptionRequestId']
                            create_by_id = datazone.get_subscription_request_details(domainIdentifier = domain_id, identifier = subscription_request_id)['createdBy']
                            updated_by_id = subscription_details['updatedBy']
                            # Find the name of the requester of the subscription and the approver/rejector
                            created_by_name = datazone.get_user_profile(domainIdentifier=domain_id, userIdentifier=create_by_id)['details']['iam']['arn']
                            updated_by_name = datazone.get_user_profile(domainIdentifier=domain_id, userIdentifier=updated_by_id)['details']['iam']['arn']
    
                    # Write the subscription details to the CSV file
                        row = {
                                'Datazone Asset Name': subscription_details['subscribedListing']['name'],
                                'Asset System Name': asset_system_name,
                                'Subscription ID': subscription_id,
                                'Subscriber Project': subscription_details['subscribedPrincipal']['project']['name'],
                                'Subscription Status': subscription_details['status'],
                                'Subscription Request ID': subscription_details['subscriptionRequestId'],
                                'Created At': subscription_details['createdAt'].strftime('%Y-%m-%d %H:%M:%S'),
                                'Created By': created_by_name,
                                'Updated At': subscription_details['updatedAt'].strftime('%Y-%m-%d %H:%M:%S'),
                                'Updated By': updated_by_name,
                                'filter_name': filter_name,
                                'filter_columns': filter_columns,
                                'filter_row_expression': filter_row_expression
                            }
                            
                        writer.writerow(row)
    
    except ClientError as e:
        print(f"An error occurred: {e}")

    # upload the csv file to s3
    try:
        s3_client = boto3.client('s3')
        bucket_name = bucket_name
        object_key = csv_file_path
        s3_client.upload_file(local_csv_file_path, bucket_name, object_key)
    except ClientError as e:
        print(f"An error occurred: {e}")


def lambda_handler(event, context):

    # read from aws lambda environment configuration variables throw an error if not found
    try:
        domain_id = os.environ['domain_id']
        project_id = os.environ['project_id']
        bucket_name = os.environ['bucket_name']
        csv_file_path = os.environ['csv_file_path']

    except KeyError as e:
        print(f"Error: {e}")

    get_datazone_subscription_details_csv(domain_id, project_id, bucket_name, csv_file_path)
    print(f"Subscription details have been written to s3://{bucket_name}/{csv_file_path}")

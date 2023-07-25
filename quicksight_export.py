import boto3
import json
import os
import logging
import datetime
import sys
import argparse

def list_quicksight_resources(account_id, region, resource_type):
    # resource_type corresponds to the Quicksight resource you want to list, 
    # and should be 'data_sets', 'analyses', or 'dashboards'. 

    quicksight_client = boto3.client('quicksight', region_name=region)
    aws_call = getattr(quicksight_client, f'list_{resource_type}') 

    try:
        response = aws_call(AwsAccountId=account_id)
        logging_msg = f'Retrieved {resource_type} list'
        logging.info(logging_msg)
        print(logging_msg)
    except Exception as error:
        error_msg = f'Failed to retrieve {resource_type} list: {error}'
        logging.error(error_msg)
        print(error_msg)
        return

    return response

def get_quicksight_resource(account_id, region, resource_type, resource_id):
    # In this case, resource_type corresponds to the Quicksight resource you
    # want to download, and should be 'data_sets', 'analyses', or 'dashboards'.
    # Note that these are plural to match other functions, despite this being
    # a singular resource. 

    quicksight_client = boto3.client('quicksight', region_name=region)

    # The API calls don't all have the same naming schema, nor do their 
    # arguments, so we have to write these individually. 
    try:
        if resource_type == 'data_sets':
            response = quicksight_client.describe_data_set(AwsAccountId=account_id, DataSetId=resource_id)
        elif resource_type == 'analyses':
            response = quicksight_client.describe_analysis_definition(AwsAccountId=account_id, AnalysisId=resource_id)
        elif resource_type == 'dashboards':
            response = quicksight_client.describe_dashboard_definition(AwsAccountId=account_id, DashboardId=resource_id)
    except Exception as error:
        logging.error(f'AWS call for {resource_type} {resource_id} failed: {error}')
        failures[resource_type] += 1
        return

    # Check to see if AWS returned an error
    if 'Error' in response:
        logging.error(f'Failed to retrieve {resource_type} {resource_id} \nAWS error code: {response["Error"]["Code"]}; Message: {response["Error"]["Message"]}')
        return

    logging.info(f'Retrieved {resource_type}')
    return response

def write_json_file(resource_type, name, file_type, json_data):
    # file_type indicates whether it's a single resource or a list of many, 
    # and should be either 'definition' or 'list'. 

    filepath = f'definitions/{resource_type}/'

    # Create subdirectory structure if needed
    try:
        if not os.path.exists(filepath):
            os.makedirs(filepath) 
            logging.info(f'Created {filepath} directory')
    except:
        error_msg = f'Failed to create {filepath} directory; exiting'
        logging.critical(error_msg)
        print(error_msg)
        finish_export()

    # Build full file name
    filename = filepath + name
    if file_type == 'list':
        filename = f'{filepath}{resource_type}-list.json'
    elif file_type == 'definition':
        filename = f'{filepath}{name}-{resource_type}.json'
    
    # Save JSON to the file
    try:
        with open(filename, 'wt') as outfile:
            json.dump(json_data, outfile, default=str, indent=4, separators=(',', ': '))
        if file_type == 'definition':
            logging.info(f'Saved "{name}" {resource_type}')
            successes[resource_type] += 1
        elif file_type == 'list':
            logging.info(f'Saved {resource_type} list')
    except:
        logging.error(f'Failed to save "{name}" {resource_type}')
        if file_type == 'definition':
            failures[resource_type] += 1

def finish_export():
    summary = f'Successfully exported {successes["data_sets"]} datasets, {successes["analyses"]} analyses, and {successes["dashboards"]} dashboards. \n Failed to export {failures["data_sets"]} datasets, {failures["analyses"]} analyses, and {failures["dashboards"]} dashboards.'
    logging.info(summary)
    print(summary)
    sys.exit()


# Configure logging
logfile_path = 'logs/'

if not os.path.exists(logfile_path):
    os.makedirs(logfile_path)

logging.basicConfig(
    filename=f'{logfile_path}{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")} UTC_Quicksight_export.log',
    filemode='w',
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.INFO 
)

# Define global counters
failures = {
    'data_sets':0,
    'analyses':0,
    'dashboards':0
}
successes = {
    'data_sets':0,
    'analyses':0,
    'dashboards':0
}

# Get command line arguments
cli = argparse.ArgumentParser()
cli.add_argument('-a', '--account_id', type=str, required=True, help='Your AWS account ID.')
cli.add_argument('-r', '--region', type=str, required=True, help='The AWS region you want to access.')
cli.add_argument('-t', '--type', type=str, help='The type of resources you want to save. "d" == datasets, "a" == analyses, "b" == dashboards.')

# Parse command line arguments and set config variables
cli_args = cli.parse_args()

account_id = cli_args.account_id
region = cli_args.region

resource_types = []
if not cli_args.type:
    # If no resource types are specified on the command line, default to all
    resource_types = ['data_sets', 'analyses', 'dashboards']
else:
    if 'd' in cli_args.type:
        resource_types.append('data_sets')
    if 'a' in cli_args.type:
        resource_types.append('analyses')
    if 'b' in cli_args.type:
        resource_types.append('dashboards')

# Define global variables
# account_id = '891208296108'
## region = 'us-east-1'

# Iterate through each type of resource
#for resource_type in ['analyses', 'dashboards']:
for resource_type in resource_types:

    # Get a list of the resources 
    list_json = list_quicksight_resources(account_id, region, resource_type)
    
    # If the list can't be retrieved, skip to the next resource type
    if not list_json:
        continue

    # Save the list to a file
    write_json_file(resource_type, '', 'list', list_json)

    # Set the proper JSON object names
    if resource_type == 'data_sets':
        json_object_name = 'DataSetSummaries'
        json_object_id = 'DataSetId'
    elif resource_type == 'analyses':
        json_object_name = 'AnalysisSummaryList'
        json_object_id = 'AnalysisId'
    elif resource_type == 'dashboards':
        json_object_name = 'DashboardSummaryList'
        json_object_id = 'DashboardId'

    # Iterate through the list
    resources = list_json[json_object_name]
    res_total = len(resources)
    for index, resource in enumerate(resources):
        
        # Retrieve the definition from AWS
        resource_json = get_quicksight_resource(account_id, region, resource_type, resource[json_object_id])

        # Check for content, sanitize the filename, and save the definition to a file
        if resource_json:
            filename = resource['Name'].replace('/','--')
            write_json_file(resource_type, filename, 'definition', resource_json)

        # Keep track of progress on the screen
        print(f'Saved {index + 1} of {res_total} {resource_type}')
        print('\033[1A', end='\x1b[2K')

# Write summary logs and exit
finish_export()

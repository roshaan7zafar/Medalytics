import boto3
import json
import datetime
import time


def lambda_handler(event, context):
    # TODO implement
    
    job_name = 'covid19-confirmed-test1'
    
    # Create a Glue client
    glue = boto3.client('glue')
    
    # Initialize the latest run status to 'STARTING' to enter the loop
    latest_run_status = 'STARTING'
    
    # Loop until the latest run status is 'SUCCEEDED' or 'FAILED'
    while latest_run_status not in ['SUCCEEDED', 'FAILED']:
        # Get the latest run of the Glue job
        response = glue.get_job_runs(
            JobName=job_name,
            MaxResults=1
        )
        #print(response)
        # Check if there are any runs returned
        if len(response['JobRuns']) > 0:
            latest_run = response['JobRuns'][0]
            print(latest_run)
            # Get the status of the latest run
            latest_run_status = latest_run['JobRunState']
            print(f'Latest run status: {latest_run_status}')
            
            # Wait for 5 seconds before checking the status again
            time.sleep(5)
        else:
            print('No job runs found')
            latest_run_status = 'FAILED' # exit the loop if there are no job runs found
    
    # Handle the completion status of the latest run
    if latest_run_status == 'SUCCEEDED':
        print('Latest run completed successfully')
    else:
        print('Latest run failed')
    
    return {
        'statusCode': 200,
        'body': json.dumps('GlueJob Status check lambda is completed')
    }

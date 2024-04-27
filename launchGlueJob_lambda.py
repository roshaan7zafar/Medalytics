import json
import boto3

eventbridge = boto3.client('events')

def lambda_handler(event, context):
    # TODO implement
    glue = boto3.client('glue')

    # Trigger the Glue job
    job_name = 'covid19-confirmed-test1'
    response = glue.start_job_run(JobName=job_name)

    # Print the response
    print(response)
    
    response1 = eventbridge.put_events(
        Entries=[
            {
                'Source': 'arn:aws:lambda:us-east-1:143176219551:function:launchCovid19GlueJob',
                'DetailType': 'Lambda Function Execution Glue Job Status Check',
                'Detail': json.dumps({'status': 'success'})
            }
        ]
    )
    print(response1['Entries'])
    
    # Return a message indicating that the job was started
    return {
        'statusCode': 200,
        'body': 'Glue job {} started'.format(job_name)
    }
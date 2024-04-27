import json
import boto3
import urllib.request
import datetime

eventbridge = boto3.client('events')

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    date = datetime.date.today().strftime('%Y%m%d')
    

    def get_s3_object_count(bucket_name,key):
        key_count = s3.list_objects_v2(Bucket=bucket_name, Prefix=key)['KeyCount']
        return key_count
    #confirmed_response = s3.get_object(Bucket='projectpro-covid19-test-data', Key='covid19/confirmed/time_series_covid19_confirmed_global.csv')
    #confirmed_file_content = confirmed_response['Body'].read().decode('utf-8')
    #s3.put_object(Bucket='projectpro-covid19-test-data', Key='covid19/archival/confirmed/'+date+'/time_series_covid19_confirmed_global.csv', Body=confirmed_file_content)
    
    #confirmed_url = 'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
    #with urllib.request.urlopen(confirmed_url) as response:
    #    data = response.read()
    #s3.put_object(Bucket='projectpro-covid19-test-data', Key='covid19/confirmed/time_series_covid19_confirmed_global.csv', Body=data)
    
    #death_response = s3.get_object(Bucket='projectpro-covid19-test-data', Key='covid19/death/time_series_covid19_deaths_global.csv')
    #death_file_content = death_response['Body'].read().decode('utf-8')
    #s3.put_object(Bucket='projectpro-covid19-test-data', Key='covid19/archival/death/'+date+'/time_series_covid19_deaths_global.csv', Body=death_file_content)
    
    #death_url = 'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
    #with urllib.request.urlopen(death_url) as response:
    #    data = response.read()
    #s3.put_object(Bucket='projectpro-covid19-test-data', Key='covid19/death/time_series_covid19_deaths_global.csv', Body=data)
    
    #recovered_response = s3.get_object(Bucket='projectpro-covid19-test-data', Key='covid19/recovered/time_series_covid19_recovered_global.csv')
    #recovered_file_content = recovered_response['Body'].read().decode('utf-8')
    #s3.put_object(Bucket='projectpro-covid19-test-data', Key='covid19/archival/recovered/'+date+'/time_series_covid19_recovered_global.csv', Body=recovered_file_content)
    
    #recovered_url = 'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'
    #with urllib.request.urlopen(recovered_url) as response:
    #    data = response.read()
    #s3.put_object(Bucket='projectpro-covid19-test-data', Key='covid19/recovered/time_series_covid19_recovered_global.csv', Body=data)
    
    covid_array = ['confirmed','deaths','recovered']
    
    for element in covid_array:
        objCount = get_s3_object_count('projectpro-covid19-test-data','covid19/'+element+'/time_series_covid19_'+element+'_global.csv')
        if objCount != 0:
            file_response = s3.get_object(Bucket='projectpro-covid19-test-data', Key='covid19/'+element+'/time_series_covid19_'+element+'_global.csv')
            
            file_content = file_response['Body'].read().decode('utf-8')
            
            s3.put_object(Bucket='projectpro-covid19-test-data', Key='covid19/archival/'+element+'/'+date+'/time_series_covid19_'+element+'_global.csv', Body=file_content)
        
        url = 'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_'+element+'_global.csv'
        
        with urllib.request.urlopen(url) as response:
            data = response.read()
            s3.put_object(Bucket='projectpro-covid19-test-data', Key='covid19/'+element+'/time_series_covid19_'+element+'_global.csv', Body=data)
    
    # Send the event to the target rule
    response = eventbridge.put_events(
        Entries=[
            {
                'Source': 'arn:aws:lambda:us-east-1:143176219551:function:copyGitHubS3',
                'DetailType': 'Lambda Function Execution Status Change',
                'Detail': json.dumps({'status': 'success'})
            }
        ]
    )
    print(response['Entries'])
    
    # Check the response and handle any errors
    if response['FailedEntryCount'] > 0:
        print(f"Failed to publish event: {response['Entries']}")
    else:
        print("Successfully published event to EventBridge bus")
    
    return {
        'statusCode': 200,
        'body': json.dumps('csv file copy from github to lambda has been completed!')
    }

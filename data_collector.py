import boto3
import os
import subprocess
import sys
import json

subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')
import yfinance 

def lambda_handler(event, context):
    fh = boto3.client("firehose", "us-east-2")
    tickers = ['SHOP','FB','BYND','NFLX','PINS','SQ','TTD','OKTA','SNAP','DDOG']
    for  i in  tickers:
        tick = yfinance.Ticker(i)
        data = tick.history(start = '2020-05-14',
                           end = '2020-05-15',
                           interval = '1m')[['High','Low']]


        data.reset_index(level=0, inplace=True)
        data['Datetime']=data['Datetime'].dt.tz_localize(None)
        data['Datetime'] = data['Datetime'].astype(str)
        data['Ticker'] = str(i)


        prejson = data.to_dict('records')

        for m in prejson:
            record =  json.dumps(m)
            fh.put_record(DeliveryStreamName="finhose", 
            Record={"Data": record.encode('utf-8')})
            
    return { 'statusCode': 200, 'body': json.dumps(f'Done! Recorded: {record}')}

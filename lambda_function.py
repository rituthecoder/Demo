import os
import json
import boto3
import csv
import io
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

region = os.environ['AWS_REGION']
customEnvironment = json.loads(os.getenv("CustomEnvironment"))
dynamodbTableName = customEnvironment.get("dynamodbTableName")

# load the exceptions for error handling
from botocore.exceptions import ClientError, ParamValidationError

# S3 client setup
s3_Client = boto3.client('s3', region_name=region)

# Creating dynamodb resources
dynamodb = boto3.resource('dynamodb', region_name=region)
dynamodbTable = dynamodb.Table(dynamodbTableName)

# Get distinct event sources in case of multiple trigger events
def get_event_source(event):
    try:
        event_source = event['Records'][0]['eventSource']
        return event_source
        
    except Exception as e:
        logger.error(f'get_event_source :: Error {e}')
        return 'Unknown'
        
# Decimal to string convertor
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        # if passed in object is instance of decimal
        # convert it to a string
        if isintance(obj, Decimal):
            return str(obj)
        # otherwise use the default behavior
        return json.JSONEncoder.default(self, obj)
            
# Read data from DynamoDB            
def get_dynamodb_item (partition_key, sort_key):
    print("Inside dynamodb item")
    try:
        dynamodb_item = dynamodbTable.get_item(
            Key = {
                'partitionKey':partition_key,
                'sortKey':sort_key
            }
        )
        logger.info(f"get_dynamodb_item :: DynamoDB item : {json.dumps(dynamodb_item['Item'], cls = DecimalEncoder)}")
        return json.dumps(dynamodb_item['Item'], cls=DecimalEncoder)
    
    except Exception as e:
        logger.error(f"get_dynamodb_item :: Error {e}")
        return None
        
# lambda handler triggered with S3 e
def lambda_handler(event, context): 
    logger.info(f"event = {json.dumps(event)}")
    event_source = get_event_source(event)
    logger.info(f"event_source = {event_source}")
    
    if event_source == 'aws:s3':
        try:
            #Read the bucket and file name
            bucket = event['Records'][0]['s3']['bucket']['name']
            key = event['Records'][0]['s3']['object']['key']
            
            print(bucket)
            print(key)
            
            #Parse the csv  
            csv = s3_Client.get_object(Bucket=bucket, Key=key)
            data = csv['Body'].read().decode('utf-8')
            orders = data.split("\n")
            orders = list(filter(None, orders))
            for order in orders:
                each_order = order.split(",")
                dynamodbTable.put_item(Item = {
                    "wo_id" : each_order[0],
                    "district" : each_order[1],
                    "lead_tech" : each_order[2],
                    "service_type" : each_order[3],
                    "Request_date" : each_order[4],
                    "labor_hours" : each_order[5],
                    "payment" : each_order[6],
                    "labor_rate" : each_order[7],
                    "labor_cost" : each_order[8],
                    "parts_cost" : each_order[9]
                })
        except Exception as e:
            logger.info(f"event = {event}")
            logger.error(f"lambda_handler :: Error{e}")
            return 'failure'
    else:
        None
    return 'success'

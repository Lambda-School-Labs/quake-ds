import json
from .USGSetl import pipe_data, HOUR

def lambda_handler(event, context):
    pipe_data(HOUR)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

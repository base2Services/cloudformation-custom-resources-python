import boto3
import os
import json


class LambdaInvoker:
    def __init__(self):
        print(f"Initialize lambda invoker")
    
    def invoke(self, payload):
        bytes_payload = bytearray()
        bytes_payload.extend(map(ord, json.dumps(payload)))
        function_name = os.environ['AWS_LAMBDA_FUNCTION_NAME']
        function_payload = bytes_payload
        client = boto3.client('lambda')
        client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=function_payload
        )

import sys
import os
import re

sys.path.append(f"{os.environ['LAMBDA_TASK_ROOT']}/lib")
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

import cr_response
import logic
import json

def lambda_handler(event, context):
    
    print(f"Received event:{json.dumps(event)}")

    lambda_response = cr_response.CustomResourceResponse(event)
    cr_params = event['ResourceProperties']
    # Validate input
    for key in ['Source', 'Destination']:
        if key not in cr_params:
            lambda_response.respond_error(f"{key} property missing")
            return
    
    # Validate input params format
    src_param = cr_params['Source']
    dst_param = cr_params['Destination']
    src_param_match = re.match(r's3:\/\/(.*?)\/(.*)', src_param)
    dst_param_match = re.match(r's3:\/\/(.*?)\/(.*)', dst_param)
    
    canned_acl = None
    if 'CannedAcl' in cr_params:
        canned_acl = cr_params['CannedAcl']
    
    if src_param_match is None or dst_param_match is None:
        lambda_response.respond_error(f"Source/Destination must be in s3://bucket/key format")
        return
    
    # get prefixes
    src_prefix = src_param_match.group(2)
    dst_prefix = dst_param_match.group(2)
    
    dst = {'Bucket': dst_param_match.group(1), 'Prefix': dst_prefix}
    
    try:
        if event['RequestType'] == 'Delete':
            logic.S3CopyLogic(context, type='clean', src=None, dst=dst, canned_acl=canned_acl).clean_destination()
            lambda_response.respond()
            return
        
        # if create request, generate physical id, both for create/update copy files
        if event['RequestType'] == 'Create':
            event['PhysicalResourceId'] = dst_param
        
        # check if source is prefix - than it is sync type
        if src_prefix.endswith('/'):
            src = {'Bucket': src_param_match.group(1), 'Prefix': src_prefix}
            logic.S3CopyLogic(context, type='sync', src=src, dst=dst, canned_acl=canned_acl).copy()
            lambda_response.respond()
        # if prefix ends with zip, we need to unpack file first
        elif src_prefix.endswith('.zip'):
            src = {'Bucket': src_param_match.group(1), 'Key': src_prefix}
            logic.S3CopyLogic(context, type='object-zip', src=src, dst=dst, canned_acl=canned_acl).copy()
            lambda_response.respond()
        # by default consider prefix as key - regular s3 object
        else:
            src = {'Bucket': src_param_match.group(1), 'Key': src_prefix}
            logic.S3CopyLogic(context, type='object', src=src, dst=dst, canned_acl=canned_acl).copy()
            lambda_response.respond()
    except Exception as e:
        message = str(e)
        lambda_response.respond_error(message)
        
    return 'OK'

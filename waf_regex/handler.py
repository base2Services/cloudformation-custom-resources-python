import sys
import os
import re
import random

sys.path.append(f"{os.environ['LAMBDA_TASK_ROOT']}/lib")
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

import cr_response
from logic import WafRegexLogic
import json

def lambda_handler(event, context):

    print(f"Received event:{json.dumps(event)}")

    lambda_response = cr_response.CustomResourceResponse(event)
    cr_params = event['ResourceProperties']
    match_name = event['LogicalResourceId']
    waf_logic = WafRegexLogic(match_name , cr_params)
    try:
        # if create request, generate physical id, both for create/update copy files
        if event['RequestType'] == 'Create':
            print("Create request")
            event['PhysicalResourceId'] = waf_logic.new_match_set()
            data = {
                "MatchID" : event['PhysicalResourceId']
            }
            lambda_response.respond(data)

        elif event['RequestType'] == 'Update':
            print("Update request")
            waf_logic.update_match_set(event['PhysicalResourceId'])
            data = {
                "MatchID" : event['PhysicalResourceId']
            }
            lambda_response.respond(data)

        elif event['RequestType'] == 'Delete':
            print(event['PhysicalResourceId'])
            waf_logic.remove_match_set(event['PhysicalResourceId'])
            print("Delete request")
            data = { }
            lambda_response.respond(data)

    except Exception as e:
        message = str(e)
        lambda_response.respond_error(message)

    return 'OK'

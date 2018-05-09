import sys
import os

sys.path.append(f"{os.environ['LAMBDA_TASK_ROOT']}/lib")
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

import cr_response
import logic
import json

def lambda_handler(event, context):

    print(f"Received event:{json.dumps(event)}")

    lambda_response = cr_response.CustomResourceResponse(event)
    cr_params = event['ResourceProperties']
    print(f"Resource Properties {cr_params}")
    # Validate input
    for key in ['Path']:
        if key not in cr_params:
            lambda_response.respond_error(f"{key} property missing")
            return

    try:
        parameter = logic.SSMSecureParameterLogic(cr_params['Path'])
        length = 16 or cr_params['Length']

        if event['RequestType'] == 'Create':
            password, version = parameter.create(
                length=length,
                update=False
            )

            event['PhysicalResourceId'] = cr_params['Path']
            lambda_response.respond(data={
                "Password": password,
                "Version": version
            })

        elif event['RequestType'] == 'Update':
            password, version = parameter.create(
                length=length,
                update=True
            )

            event['PhysicalResourceId'] = cr_params['Path']
            lambda_response.respond(data={
                "Password": password,
                "Version": version
            })

        elif event['RequestType'] == 'Delete':
            parameter.delete()
            lambda_response.respond()

    except Exception as e:
        message = str(e)
        lambda_response.respond_error(message)

    return 'OK'

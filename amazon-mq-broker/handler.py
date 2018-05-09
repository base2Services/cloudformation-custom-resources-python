import sys
import os
import json

sys.path.append(f"{os.environ['LAMBDA_TASK_ROOT']}/lib")
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

import cr_response
import logic
import lambda_invoker

def lambda_handler(event, context):

    print(f"Received event:{json.dumps(event)}")

    lambda_response = cr_response.CustomResourceResponse(event)
    cr_params = event['ResourceProperties']

    # Validate input
    for key in ['MultiAZ', 'InstanceType', 'Username', 'Password', 'SecurityGroups', 'Subnets']:
        if key not in cr_params:
            lambda_response.respond_error(f"{key} property missing")
            return

    try:
        broker = logic.AmazonMQBrokerLogic(cr_params['Name'])
        if event['RequestType'] == 'Create':
            if ('WaitComplete' in event) and (event['WaitComplete']):
                result = broker.wait_broker_status(event['PhysicalResourceId'], context)

                if result is None:
                    invoke = lambda_invoker.LambdaInvoker()
                    invoke.invoke(event)
                elif result:
                    lambda_response.respond(data=event['Data'])
                elif not result:
                    lambda_response.respond_error(f"Creation of AmazonMQ {event['PhysicalResourceId']} failed.")

            else:
                response = broker.create(
                    multi_az=cr_params['MultiAZ'],
                    instance_type=cr_params['InstanceType'],
                    user=cr_params['Username'],
                    password=cr_params['Password'],
                    security_groups=cr_params['SecurityGroups'],
                    subnets=cr_params['Subnets']
                )

                event['PhysicalResourceId'] = response['BrokerId']
                event['Data'] = response
                event['WaitComplete'] = True
                invoke = lambda_invoker.LambdaInvoker()
                invoke.invoke(event)

        elif event['RequestType'] == 'Update':
            comparision = broker.compare_broker_properites(event['PhysicalResourceId'], event['ResourceProperties'])
            if not comparision:
                lambda_response.respond_error("AmazonMQ resource cannot be updated. Create a new resource if changes are required.")
            else:
                response = broker.get_broker_data(event['PhysicalResourceId'], event['ResourceProperties']['MultiAZ'])
                lambda_response.respond(data=response)

        elif event['RequestType'] == 'Delete':
            broker.delete(event['PhysicalResourceId'])
            lambda_response.respond()

    except Exception as e:
        message = str(e)
        lambda_response.respond_error(message)

    return 'OK'

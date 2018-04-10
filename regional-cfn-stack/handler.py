import boto3
import json
import os
import sys

sys.path.append(f"{os.environ['LAMBDA_TASK_ROOT']}/lib")
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

import cr_response
import stack_manage
import lambda_invoker
import traceback

create_stack_success_states = ['CREATE_COMPLETE']
update_stack_success_states = ['CREATE_COMPLETE', 'UPDATE_COMPLETE']
delete_stack_success_states = ['DELETE_COMPLETE']

create_stack_failure_states = ['CREATE_FAILED',
                               'DELETE_FAILED',
                               'UPDATE_FAILED',
                               'ROLLBACK_FAILED',
                               'DELETE_COMPLETE',
                               'ROLLBACK_COMPLETE']
update_stack_failure_states = ['CREATE_FAILED', 'DELETE_FAILED', 'UPDATE_FAILED', 'ROLLBACK_COMPLETE','UPDATE_ROLLBACK_COMPLETE']
delete_stack_failure_states = ['DELETE_FAILED']


def respond_disabled_region(region, payload):
    cfn_response = cr_response.CustomResourceResponse(payload)
    payload['PhysicalResourceId'] = f"Disabled{region.replace('-','')}{payload['ResourceProperties']['StackName']}"
    cfn_response.response['Status'] = 'SUCCESS'
    cfn_response.respond()
    return


def create_update_stack(cmd, payload):
    if 'Capabilities' not in payload['ResourceProperties']:
        payload['ResourceProperties']['Capabilities'] = 'CAPABILITY_IAM'
    
    # compile stack parameters
    stack_params = {}
    for key, value in payload['ResourceProperties'].items():
        if key.startswith('StackParam_'):
            param_key = key.replace('StackParam_', '')
            param_value = value
            stack_params[param_key] = param_value
    
    # instantiate and use management handler
    manage = stack_manage.StackManagement()
    
    on_failure = 'DELETE'
    if 'OnFailure' in payload['ResourceProperties']:
        on_failure = payload['ResourceProperties']['OnFailure']
    
    stack_id = ''
    if cmd == 'create':
        stack_id = manage.create(
            payload['ResourceProperties']['Region'],
            payload['ResourceProperties']['StackName'],
            payload['ResourceProperties']['TemplateUrl'],
            stack_params,
            payload['ResourceProperties']['Capabilities'].split(','),
            on_failure
        )
    elif cmd == 'update':
        stack_id = payload['PhysicalResourceId']
        result = manage.update(
            payload['ResourceProperties']['Region'],
            stack_id,
            payload['ResourceProperties']['TemplateUrl'],
            stack_params,
            payload['ResourceProperties']['Capabilities'].split(','),
        )
        # no updates to be performed
        if result is None:
            cfn_response = cr_response.CustomResourceResponse(payload)
            cfn_response.respond()
            return None
    else:
        raise 'Cmd must be create or update'
    
    return stack_id


def delete_stack(payload):
    manage = stack_manage.StackManagement()
    region = payload['ResourceProperties']['Region']
    stack_id = payload['PhysicalResourceId']
    manage.delete(region, payload['ResourceProperties']['StackName'])
    return stack_id


def wait_stack_states(success_states, failure_states, lambda_payload, lambda_context):
    """
    Wait for stack states, either be it success or failure. If none of the states
    appear and lambda is running out of time, it will be re-invoked with lambda_payload
    parameters
    :param lambda_context:
    :param stack_id:
    :param success_states:
    :param failure_states:
    :param lambda_payload:
    :return:
    """
    manage = stack_manage.StackManagement()
    result = manage.wait_stack_status(
        lambda_payload['ResourceProperties']['Region'],
        lambda_payload['PhysicalResourceId'],
        success_states,
        failure_states,
        lambda_context
    )
    
    # in this case we need to restart lambda execution
    if result is None:
        invoke = lambda_invoker.LambdaInvoker()
        invoke.invoke(lambda_payload)
    else:
        # one of the states is reached, and reply should be sent back to cloud formation
        cfn_response = cr_response.CustomResourceResponse(lambda_payload)
        cfn_response.response['PhysicalResourceId'] = lambda_payload['PhysicalResourceId']
        cfn_response.response['Status'] = result.status
        cfn_response.response['Reason'] = result.reason
        cfn_response.response['StackId'] = lambda_payload['StackId']
        cfn_response.respond()


def lambda_handler(payload, context):
    # if lambda invoked to wait for stack status
    print(f"Received event:{json.dumps(payload)}")
    
    # handle disable region situation
    if 'EnabledRegions' in payload['ResourceProperties']:
        region_list = payload['ResourceProperties']['EnabledRegions'].split(',')
        current_region = payload['ResourceProperties']['Region']
        print(f"EnabledRegions: {region_list}. Current region={current_region}")
        
        if current_region not in region_list:
            # if this is create request just skip
            if payload['RequestType'] == 'Create' or payload['RequestType'] == 'Update':
                print(f"{current_region} not enabled, skipping")
                # report disabled
                # in case of region disable (update), physical record changes, so cleanup delete request is
                # sent subsequently via Cf, which will delete the stack
                respond_disabled_region(current_region, payload)
                return
    
    
    
    # lambda was invoked by itself, we just have to wait for stack operation to be completed
    if ('WaitComplete' in payload) and (payload['WaitComplete']):
        print("Waiting for stack status...")
        if payload['RequestType'] == 'Create':
            wait_stack_states(
                create_stack_success_states,
                create_stack_failure_states,
                payload,
                context
            )
        
        elif payload['RequestType'] == 'Update':
            wait_stack_states(
                update_stack_success_states,
                update_stack_failure_states,
                payload,
                context
            )
        
        elif payload['RequestType'] == 'Delete':
            wait_stack_states(
                delete_stack_success_states,
                delete_stack_failure_states,
                payload,
                context
            )
    
    # lambda was invoked directly by cf
    else:
        # depending on request type different handler is called
        print("Executing stack CRUD...")
        stack_id = None
        if 'PhysicalResourceId' in payload:
            stack_id = payload['PhysicalResourceId']
        try:
            manage = stack_manage.StackManagement()
            stack_name = payload['ResourceProperties']['StackName']
            region = payload['ResourceProperties']['Region']
            stack_exists = manage.stack_exists(region, stack_name)
            
            if payload['RequestType'] == 'Create':
                # stack exists, create request => update
                # stack not exists, create request => create
                if stack_exists:
                    print(f"Create request came for {stack_name}, but it already exists in {region}, updating...")
                    payload['RequestType'] = 'Update'
                    lambda_handler(payload, context)
                    return
                else:
                    stack_id = create_update_stack('create', payload)
            
            elif payload['RequestType'] == 'Update':
                # stack exists, update request => update
                # stack not exists, update request => create
                if stack_exists:
                    stack_id = create_update_stack('update', payload)
                    if stack_id is None:
                        # no updates to be performed
                        return
                else:
                    print(f"Update request came for {stack_name}, but it does not exist in {region}, creating...")
                    payload['RequestType'] = 'Create'
                    lambda_handler(payload, context)
                    return
            
            elif payload['RequestType'] == 'Delete':
                # stack exists, delete request => delete
                # stack not exists, delete request => report ok
                # for delete we are interested in actual stack id
                stack_exists = manage.stack_exists(region, stack_id)
                if stack_exists:
                    delete_stack(payload)
                else:
                    # reply with success
                    print(f"Delete request came for {stack_name}, but it is nowhere to be found...")
                    cfn_response = cr_response.CustomResourceResponse(payload)
                    cfn_response.response['Reason'] = 'CloudFormation stack has not been found, may be removed manually'
                    cfn_response.respond()
                    return
            
            # if haven't moved to other operation, set payloads stack id to created/updated stack and wait
            # for appropriate stack status
            payload['PhysicalResourceId'] = stack_id
            payload['WaitComplete'] = True
            invoker = lambda_invoker.LambdaInvoker()
            invoker.invoke(payload)
        
        except Exception as e:
            print(f"Exception:{e}\n{str(e)}")
            print(traceback.format_exc())
            cfn_response = cr_response.CustomResourceResponse(payload)
            if 'PhysicalResourceId' in payload:
                cfn_response.response['PhysicalResourceId'] = payload['PhysicalResourceId']
            cfn_response.response['Status'] = 'FAILED'
            cfn_response.response['Reason'] = str(e)
            cfn_response.respond()
            raise e

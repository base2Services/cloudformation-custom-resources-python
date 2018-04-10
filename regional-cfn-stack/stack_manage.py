import boto3
from functools import reduce
import time
import json

class StackStatus:
    def __init__(self, success, reason):
        self.status = 'SUCCESS' if success else 'FAILED'
        self.reason = reason


class StackManagement:
    def __init__(self):
        print("Initialize stack management handler object")
    
    def create(self, region, name, url, params, capabilities, on_failure):
        cfn_client = boto3.client('cloudformation', region_name=region)
        s_params = params
        
        # log stack creation info
        print(f"Creating stack\n\tName:{name}\n\tTemplate:{url}\n\tParams:{s_params}\n\tCapabilities:{capabilities}")
        response = cfn_client.create_stack(
            StackName=name,
            TemplateURL=url,
            Parameters=list(map(lambda x: {'ParameterKey': x[0], 'ParameterValue': x[1]}, params.items())),
            Capabilities=capabilities,
            OnFailure=on_failure
        )
        return response['StackId']
    
    def stack_exists(selfs, region, stack_name):
        cfn_client = boto3.client('cloudformation', region_name=region)
        try:
            stack_details = cfn_client.describe_stacks(StackName=stack_name)['Stacks'][0]
            return stack_details['StackStatus'] != 'DELETE_COMPLETE'
        except Exception as e:
            if 'does not exist' in e.response['Error']['Message']:
                return False
            else:
                raise e
    
    def update(self, region, stack_id, url, params, capabilities):
        cfn_client = boto3.client('cloudformation', region_name=region)
        s_params = params
        
        # log stack update info
        print(f"Updating stack\n\t" +
              f"StackId:{stack_id}\n\tTemplate:{url}\n\tParams:{s_params}\n\tCapabilities:{capabilities}")
        try:
            response = cfn_client.update_stack(
                StackName=stack_id,
                TemplateURL=url,
                Parameters=list(map(lambda x: {'ParameterKey': x[0], 'ParameterValue': x[1]}, params.items())),
                Capabilities=capabilities
            )
        except Exception as e:
            if (not 'Error' in e.response):
                raise e
            if (not 'Message' in e.response['Error']):
                raise e
            if ('No updates are to be performed' in e.response['Error']['Message']):
                print("No updates for stack {stack_id}")
                return None
            else:
                raise e
        
        return stack_id
    
    def delete(self, region, stack_id):
        cfn_client = boto3.client('cloudformation', region_name=region)
        print(f"Deleting stack: {stack_id}")
        cfn_client.delete_stack(StackName=stack_id)
    
    def wait_stack_status(self, region, stack_id, success_states, failure_states, lambda_context):
        cfn_client = boto3.client('cloudformation', region_name=region)
        print(f"Monitoring stack:{stack_id}\n\tSUCCESS states={success_states}\n\tFAILURE states={failure_states}")
        while True:
            stack_details = cfn_client.describe_stacks(StackName=stack_id)['Stacks'][0]
            stack_status = stack_details['StackStatus']
            
            print(f"Stack status: {stack_status}")
            if stack_status in success_states:
                print(f"Matched {stack_status} - OK ")
                return StackStatus(True, '')
            elif stack_status in failure_states:
                print(f"Matched {stack_status} - ERROR ")
                return StackStatus(False, self.get_failure_reason(region, stack_id))
            elif lambda_context.get_remaining_time_in_millis() < 10000:
                print(f"Less than 10 seconds left of Lambda execution time, exiting with empty hands")
                return None
            else:
                print(f"Waiting for 5 seconds, time remaining" +
                      f"in this lambda execution {lambda_context.get_remaining_time_in_millis()}ms")
                time.sleep(5)
    
    def get_failure_reason(self, region, stack_id):
        """
        Get stack failure reason by reading stack events. Any event with status
        *FAILED will be considered as failure reason. Only last 3 status reasons
        are considered relevant, as other may be older
        
        :param region:
        :param stack_id:
        :param cut_off_op:
        :return:
        """
        cfn_client = boto3.client('cloudformation', region_name=region)
        events = cfn_client.describe_stack_events(StackName=stack_id)['StackEvents']
        status_reason = ''
        count = 0
        for event in events:
            # only last 3 status reasons will be returned, more than enough
            # for troubleshooting purposes
            if count == 3:
                return status_reason
            
            event_status = event['ResourceStatus']
            if event_status.endswith('FAILED'):
                count = count + 1
                status_reason = status_reason + event['ResourceStatusReason']
        
        return status_reason

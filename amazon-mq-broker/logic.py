import boto3
import os
import time

class AmazonMQBrokerLogic:

    def __init__(self, broker_name):
        self.broker_name = broker_name

    def create(self, multi_az, instance_type, user, password, security_groups, subnets):
        print(f"Creating AMQ instance {self.broker_name}")
        deployment_mode = "ACTIVE_STANDBY_MULTI_AZ" if multi_az.lower() == "true" else "SINGLE_INSTANCE"

        client = boto3.client('mq')
        response = client.create_broker(
            AutoMinorVersionUpgrade=False,
            BrokerName=self.broker_name,
            DeploymentMode=deployment_mode,
            EngineType='ACTIVEMQ',
            EngineVersion='5.15.0',
            HostInstanceType=instance_type,
            PubliclyAccessible=False,
            SecurityGroups=security_groups,
            SubnetIds=subnets,
            Users=[
                {
                    'ConsoleAccess': True,
                    'Password': password,
                    'Username': user
                }
            ]
        )
        print(f"Broker Id: {response['BrokerId']} Broker Arn: {response['BrokerArn']}")
        active = self.endpoint(response['BrokerId'],1)
        response.update({'Active': active})

        standby = self.endpoint(response['BrokerId'],2) if multi_az.lower() == "true" else "NONE"
        response.update({'Standby': standby})

        print(f"Creating Amazon MQ instance\n{response}")
        return response

    def wait_broker_status(self, id, lambda_context):
        client = boto3.client('mq')

        while True:
            response = client.describe_broker(BrokerId=id)
            state = response['BrokerState']

            print(f"Broker state: {state}")
            if state == 'RUNNING':
                print(f"Matched {state} - OK ")
                return True
            elif state == 'CREATION_FAILED':
                print(f"Matched {state} - ERROR ")
                return False
            elif lambda_context.get_remaining_time_in_millis() < 10000:
                print(f"Less than 10 seconds left of Lambda execution time, exiting with empty hands")
                return None
            else:
                print(f"Waiting for 5 seconds, time remaining" +
                      f"in this lambda execution {lambda_context.get_remaining_time_in_millis()}ms")
                time.sleep(5)

    def compare_broker_properites(self, id, properties):
        client = boto3.client('mq')
        response = client.describe_broker(BrokerId=id)

        deployment_mode = "ACTIVE_STANDBY_MULTI_AZ" if properties['MultiAZ'].lower() == "true" else "SINGLE_INSTANCE"

        if  (properties['SecurityGroups'] == response['SecurityGroups']) and \
            (properties['Subnets'] == response['SubnetIds']) and \
            (properties['InstanceType'] == response['HostInstanceType']) and \
            (deployment_mode == response['DeploymentMode']) and \
            (properties['Name'] == response['BrokerName']):
            return True
        else:
            return False

    def get_broker_data(self, id, multi_az):
        data = {}
        client = boto3.client('mq')
        response = client.describe_broker(BrokerId=id)

        data.update({'BrokerId': response['BrokerId']})
        data.update({'BrokerArn': response['BrokerArn']})

        active = self.endpoint(response['BrokerId'],1)
        data.update({'Active': active})

        standby = self.endpoint(response['BrokerId'],2) if multi_az.lower() == "true" else "NONE"
        data.update({'Standby': standby})

        return data

    def delete(self,id):
        client = boto3.client('mq')
        client.delete_broker(
            BrokerId=id
        )

    def endpoint(self,id,n):
        return f"{id}-{n}.mq.{os.environ['AWS_REGION']}.amazonaws.com"

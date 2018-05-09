import boto3
import string
import random

class SSMSecureParameterLogic:

    def __init__(self, path):
        self.path = path

    def create(self, length, update):
        password = self.generate_password(length)
        client = boto3.client('ssm')
        response = client.put_parameter(
            Name=self.path,
            Value=password,
            Overwrite=update,
            Type='SecureString'
        )
        return password, response['Version']

    def delete(self):
        client = boto3.client('ssm')
        client.delete_parameter(
            Name=self.path
        )

    def generate_password(self, length):
        print(f"Generating a new password {length} chars long")
        return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

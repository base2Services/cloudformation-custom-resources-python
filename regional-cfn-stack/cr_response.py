import logging
from urllib.request import urlopen, Request, HTTPError, URLError
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class CustomResourceResponse:
    def __init__(self, request_payload):
        self.payload = request_payload
        self.response = {
            "StackId": request_payload["StackId"],
            "RequestId": request_payload["RequestId"],
            "LogicalResourceId": request_payload["LogicalResourceId"],
            "Status": 'SUCCESS',
        }
    
    def respond(self):
        event = self.payload
        response = self.response
        ####
        #### copied from https://github.com/ryansb/cfn-wrapper-python/blob/master/cfn_resource.py
        ####
        
        if event.get("PhysicalResourceId", False):
            response["PhysicalResourceId"] = event["PhysicalResourceId"]
        
        logger.debug("Received %s request with event: %s" % (event['RequestType'], json.dumps(event)))
        
        serialized = json.dumps(response)
        logger.info(f"Responding to {event['RequestType']} request with: {serialized}")
        
        req_data = serialized.encode('utf-8')
        
        req = Request(
            event['ResponseURL'],
            data=req_data,
            headers={'Content-Length': len(req_data),'Content-Type': ''}
        )
        req.get_method = lambda: 'PUT'
        
        try:
            urlopen(req)
            logger.debug("Request to CFN API succeeded, nothing to do here")
        except HTTPError as e:
            logger.error("Callback to CFN API failed with status %d" % e.code)
            logger.error("Response: %s" % e.reason)
        except URLError as e:
            logger.error("Failed to reach the server - %s" % e.reason)

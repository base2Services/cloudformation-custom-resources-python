# CloudFormation custom resource catalogue

Collection of Cloud Formation custom resources written in python 3.6, result
of months of continuous efforts to automate infrastructure management trough
AWS CloudFormation. You may find some of these CloudFormation resources obsolete,
as AWS team fills in the gaps. There is also some more complex ones, or developed
to suite specific needs, such as copying s3 objects between buckets


[Collection of custom resources for Node.js can be found here](https://github.com/base2Services/cloudformation-custom-resources-nodejs)


## Usage

Clone the repository, install the dependencies, and zip up the folder as Lambda Deployment package.
Note that `boto3` is not packaged as dependency, as it is available in Lambda runtime itself by default.

```
git clone https://github.com/base2Services/cloudformation-custom-resources-python.git && \
cd cloudformation-custom-resources-python && \
docker run --rm -v $PWD:/src -w /src python:3.6 pip install -r requirements.txt -t . && \
zip -r ~/cfn-ccr-python3.6.zip .
```

## Custom resources

### Creating CloudFormation stack in specific region

It is easy to create sub-stacks in CloudFormation as long as they are in same region.
In some cases, there is need to create stack in region different than region where
parent stack is being create, or for example, to create same stack in multiple regions.
Such (sub)stack lifecycle can be controlled via custom resource having it's code in
`regional-cfn-stack` folder

handler: `regional-cfn-stack/handler.lambda_handler`
runtime: `python3.6`

Required parameters:
- `Region` - AWS Region to create stack in
- `StackName` - Name of the stack to be created
- `TemplateUrl` - S3 Url of stack template
- `Capabilities` - Comma seperated list of capabilities. Set to empty value if no IAM capabilities required.
- `EnabledRegions` - Comma separated list of regions that stack is allowed to be created in.
 Useful when passing this list is template parameters.


Optional parameters:
- `StackParam_Key` - Will pass value of this param down to stack's `Key` parameter
- `OnFailure` - Behaviour on stack creation failure. Accepted values are `DO_NOTHING`,`ROLLBACK` and `DELETE`

### Copy or unpack objects between S3 buckets

This custom resource allows copying from source to destination s3 buckets. For source, if you provide prefix
(without trailing slash), all objects under that prefix will be copied. Alternatively, if you provide s3 object
with `*.zip` extensions, this object will be unpacked before it's files are unpacked to target bucket / prefix.
Please note that this lambda function design does not include recursive calls if lambda is timing out, thus it does not
permit mass file unpacking, but is rather designed for deployment of smaller files, such as client side web applications.

handler: `3-copy/handler.lambda_handler`
runtime:  `python3.6`

Required parameters:

- `Source` - Source object/prefix/zip-file in `s3://bucket-name/path/to/prefix/or/object.zip` format
- `Destination` - Destination bucket and prefix in `s3://bucket-name/destination-prefix` format
- `CannedAcl` - Canned ACL for created objects in destination
No optional parameters.

### Create Regex Waf Rules

This custom resource allows create/update/delete match regex rules with regex a pattern set.

handler: `waf_regex/handler.lambda_handler`
runtime: `python3.6`

Required parameters:

- `Name` - Custom Resource name.
- `RegexPatterns` - List format, regex pattern to match.
- `Type` - The part of the web request that you want AWS WAF to search for a specified string
- `Data` - Data such as when the value of Type is HEADER , enter the name of the header that you want AWS WAF to search, for example, User-Agent or Referer
- `Transform` - Text transformations eliminate some of the unusual formatting that attackers use in web requests in an effort to bypass AWS WAF.
Implementation require to be serialised with other waf condition.
### AmazonMQ Broker

This custom resource creates a AmazonMQ broker instance.

**NOTE:** This resource cannot be updated. If a change to the instance is required such as Instance Type, a new broker resource must be created.

handler: `amazon-mq-broker/handler.lambda_handler`
runtime:  `python3.6`

Required parameters:

- `Name` - Unique name given to the broker
- `SecurityGroups` - Array of security group ids
- `Subnets` - Array of subnets ids
- `MultiAZ` - String boolean [ 'true', 'false' ]
- `InstanceType` - valid values [ 'mq.t2.micro', 'mq.m4.large' ]
- `Username` - Username for the amq user
- `Password` - Password for the amq user. Must be 12-250 characters long

No optional parameters.

Returned Values:

- `Active` - Active AmazonMQ endpoint
- `Stanby` - Standby AmazonMQ endpoint
- `BrokerId` - Id of the AmazonMQ Broker
- `BrokerArn` - Arn of the broker

IAM Permissions:

```json
{
  "Statement":
  [
    {
      "Effect": "Allow",
      "Action":
      [
        "mq:*",
        "ec2:CreateNetworkInterface",
        "ec2:CreateNetworkInterfacePermission",
        "ec2:DeleteNetworkInterface",
        "ec2:DeleteNetworkInterfacePermission",
        "ec2:DetachNetworkInterface",
        "ec2:DescribeInternetGateways",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribeNetworkInterfacePermissions",
        "ec2:DescribeRouteTables",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSubnets",
        "ec2:DescribeVpcs",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "lambda:InvokeFunction"
      ],
      "Resource": ["*"]
    }
  ]
}
```

### Auto generated secure ssm parameters

This custom resource generates a random string `[a-z][A-Z][0-9]` a definable length. The string is then return to the cfn stack and can then be passed into other resources requiring a password. The resource can be updated generating a new password and updating the SSM parameter and returning the new password by passing a dummy parameter into the custom resource.

handler: `ssm-secure-parameter/handler.lambda_handler`
runtime:  `python3.6`

Required parameters:

- `Path` - SSM parameter path e.g. `/app/env/password`

Optional parameters:

- `Length` - Length of the auto generated password. Defaults to 16 characters

Returned Values:

- `Password` - The password generated by the resource

IAM Permissions:

```json
{
  "Statement":
  [
    {
      "Effect": "Allow",
      "Action":
      [
        "ssm:PutParameter",
        "ssm:DeleteParameter",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": ["*"]
    }
  ]
}
```

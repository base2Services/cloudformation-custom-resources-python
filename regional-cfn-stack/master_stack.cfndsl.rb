require 'json'

CloudFormation do

  Parameter("SourceBucket") do
     Type 'String'
  end

  IAM_Role('BackingLambdaRole') do
      ManagedPolicyArns(['arn:aws:iam::aws:policy/AdministratorAccess'])
  end

  Resource('BackingLambda') do
    Type 'AWS::Lambda::Function'
    Property('Code',
      {
        'S3Bucket'=> Ref('SourceBucket'),
        'S3Key'=> 'cloudformation/lambdas/regionalcfn.zip'
      }
    )
    Property 'Runtime','python3.6'
    Property 'Handler','handler.lambda_handler'
    Property 'Timeout', 10
    Property 'Role', FnGetAtt('BackingLambdaRole','Arn')
  end

  regions = JSON.parse(`aws ec2 describe-regions --out json --query 'Regions[].RegionName'`)

  regions.each do |region|
    Resource("RegionalBucket#{region.gsub('-','')}") do
      Type 'Custom::SingleBucket'
      Property 'ServiceToken',FnGetAtt('BackingLambda','Arn')
      Property 'StackName', "RegionalBucket#{region.gsub('-','')}"
      Property 'Region', region
      Property('TemplateUrl',
        FnJoin('',["https://",
          Ref('SourceBucket'),
          ".s3.amazonaws.com/cloudformation/regional_test_stack.json"
        ]))

      Property 'EnabledRegions','us-east-1,ap-southeast-2,eu-central-1,ca-central-1'
      Property('StackParam_BucketName',
        FnJoin('',[
          Ref('AWS::AccountId'),
          '.',
          Ref('AWS::Region'),
          ".regionalcf.test"
        ])
      )
      # hack to get custom resource always updated
      Property 'StackParam_Second',"#{Time.now.to_i}"
    end
  end
end

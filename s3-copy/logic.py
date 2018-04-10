import boto3
import os
import zipfile
import glob
import logging
import shutil
import magic

logger = logging.getLogger()
logger.setLevel(logging.INFO)



class S3CopyLogic:
    
    
    ### src - dict with Bucket and Key elements
    ### destination - dict with Bucket and Key elements
    ###
    def __init__(self, context, type, src, dst, canned_acl):
        self.context = context
        self.type = type
        self.src = src
        self.dst = dst
        self.local_filename = None
        self.local_download_path = f"/tmp/cache/{self.context.aws_request_id}"
        self.local_prefix_unzip = f"/tmp/cache/{self.context.aws_request_id}/unpacked"
        self.local_prefix = f"/tmp/cache/{self.context.aws_request_id}/upload"
        self.canned_acl = canned_acl
        self.mime = magic.Magic(mime=True)


    def apply_acl(self, s3_resource, path):
        if self.canned_acl is not None:
            logger.info(f"Apply canned acl {self.canned_acl} for {path}")
            acl = s3_resource.ObjectAcl(self.dst['Bucket'], path)
            acl.put(ACL=self.canned_acl)
    
    def copy(self):
        shutil.rmtree(self.local_download_path, ignore_errors=True)
        
        if self.type == 'object-zip':
            self.download_object_unpack_zip_upload()
        elif self.type == 'object':
            self.download_object_upload()
        elif self.type == 'sync':
            self.download_prefix_upload_prefix()
        else:
            raise f"{self.type} type not supported"
    
    def clean_destination(self):
        client = boto3.client('s3')
        bucket = boto3.resource('s3').Bucket(self.dst['Bucket'])
        resp = client.list_objects_v2(Bucket=self.dst['Bucket'], Prefix=self.dst['Prefix'])
        logger.info(resp)
        if resp['KeyCount'] > 0:
            # delete api allow deletion of multiple objects in single call
            bucket.delete_objects(Delete={'Objects': list(map(lambda x: {'Key': x['Key']}, resp['Contents']))})
            
            while resp['IsTruncated']:
                resp = client.list_objects_v2(Bucket=self.dst['Bucket'],
                                              Prefix=self.dst['Prefix'],
                                              ContinuationToken=resp['NextContinuationToken'])
                bucket.delete_objects(Delete={'Objects': list(map(lambda x: {'Key': x['Key']}, resp['Contents']))})
    
    def download_object_unpack_zip_upload(self):
        self.download_object()
        self.unpack_zip()
        self.upload(self.local_prefix_unzip)
    
    def download_object_upload(self):
        self.download_object()
        self.upload(self.local_download_path)
    
    def download_prefix_upload_prefix(self):
        self.download_prefix()
        self.upload(self.local_download_path)
    
    # Download whole bucket prefix
    def download_prefix(self):
        client = boto3.client('s3')
        bucket = boto3.resource('s3').Bucket(self.src['Bucket'])
        objects = []
        resp = client.list_objects_v2(Bucket=self.src['Bucket'], Prefix=self.src['Prefix'])
        objects += map(lambda x: x['Key'], resp['Contents'])
        while resp['IsTruncated']:
            resp = client.list_objects_v2(Bucket=self.src['Bucket'],
                                          Prefix=self.src['Prefix'],
                                          ContinuationToken=resp['NextContinuationToken'])
            objects += map(lambda x: x['Key'], resp['Contents'])
        
        for object in objects:
            local_path = self.local_download_path + "/"
            local_path += object.replace(self.src['Prefix'], '')
            logger.info(f"s3://{self.src['Bucket']}/{object} -> {local_path}")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            bucket.download_file(object, local_path)
    
    # Download S3 object to lambda /tmp under current request
    def download_object(self):
        local_filename = os.path.basename(self.src['Key'])
        self.local_filename = f"{self.local_download_path}/{local_filename}"
        os.makedirs(os.path.dirname(self.local_filename), exist_ok=True)
        s3 = boto3.resource('s3')
        logger.info(f"s3://{self.src['Bucket']}/{self.src['Key']} -> {self.local_filename}")
        s3.Bucket(self.src['Bucket']).download_file(self.src['Key'], self.local_filename)
    
    # Unpack downloaded zip archive
    def unpack_zip(self):
        os.makedirs(os.path.dirname(self.local_prefix_unzip), exist_ok=True)
        logger.info(f"Unpack {self.local_filename} to {self.local_prefix_unzip}")
        zip_ref = zipfile.ZipFile(self.local_filename, 'r')
        zip_ref.extractall(self.local_prefix_unzip)
        zip_ref.close()
    
    # Upload files to destination
    def upload(self, path):
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client('s3')
        bucket = s3_resource.Bucket(self.dst['Bucket'])
        logger.info(f"Uploading from {path}")
        for local_path in glob.glob(f"{path}/**/*", recursive=True):
            if not os.path.isdir(local_path):
                content_type = self.mime.from_file(local_path)
                destination_key = self.dst['Prefix']
                if not destination_key[-1] == '/':
                    destination_key += '/'
                destination_key += local_path.replace(f"{path}/", '')
                logger.info(f"{local_path} -> s3://{self.dst['Bucket']}/{destination_key}")
                bucket.upload_file(local_path, destination_key, ExtraArgs={
                    "Metadata": {
                        "Content-Type": content_type
                    }
                })
                self.apply_acl(s3_resource,  destination_key)
                

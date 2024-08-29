# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import json
from botocore.exceptions import ClientError
import botocore.exceptions as exceptions 


# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

# Add utility code here
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')
sqs = boto3.client('sqs')
glacier = boto3.client('glacier')

def main():
    print('....Move files to glacier...')
    while True:
        # If the user is free_user, the run.py will send out the message to queue
        response = sqs.receive_message(
            QueueUrl=config['aws']['ArchiveQueueUrl'], 
            MaxNumberOfMessages=1, 
            WaitTimeSeconds=20)
        print("Receive message sucessfully")


        try:
            message = response['Messages'][0]
            message_body = message['Body']
            receipt_handle = message['ReceiptHandle']
            
        except KeyError: # queue is empty
            continue

        fixed_json_string = message_body.replace("'", '"')
        message_body_dict = json.loads(fixed_json_string)
        user_id = message_body_dict['user_id']
        job_id = message_body_dict['job_id']
        s3_key_result_file = message_body_dict['s3_key_result_file']
        print(job_id)
        _, _, _, _, role, _, _ = helpers.get_user_profile(id=user_id) # Shitty utility return value
        if role == 'premium_user':   # in case free_user update to premium user
            sqs.delete_message(
                QueueUrl=config['aws']['ArchiveQueueUrl'], 
                ReceiptHandle=receipt_handle)
            continue
        
        try:
            response = s3.get_object(
                Bucket=config['aws']['AWS_S3_RESULTS_BUCKET'],
                Key=s3_key_result_file)
            result_file_object = response['Body'].read()
        except exceptions.ClientError as e: 
            print(f"{e}\nBucket:{config['aws']['AWS_S3_RESULTS_BUCKET']}\nKey:{s3_key_result_file}")
            continue
        
        
        try:
            response = glacier.upload_archive(
                vaultName=config['aws']['AWS_S3_Glacier_Bucket_Name'],
                body=result_file_object
            )
            location, archive_id = response['location'], response['archiveId']
            print(f"Archive uploaded successfully: {archive_id} at {location}")
        except ClientError as e:
            print(f"An error occurred during upload: {e.response['Error']['Message']}")
            continue

        # Update databse with arichive id
        dynamodb.update_item(
                TableName=config['aws']['DynamodbName'],
                Key={'job_id': {'S': job_id}}, 
                ExpressionAttributeValues={
                    ':id': {'S': archive_id}
                }, 
                UpdateExpression='SET results_file_archive_id = :id REMOVE s3_key_result_file'
            )
        
        # delete results file from s3
        s3.delete_object(
                Bucket=config['aws']['AWS_S3_RESULTS_BUCKET'],
                Key=s3_key_result_file)

        # delete message from archive queue
        sqs.delete_message(
                QueueUrl=config['aws']['ArchiveQueueUrl'], 
                ReceiptHandle=receipt_handle)

if __name__ == '__main__': 
    main()

### EOF
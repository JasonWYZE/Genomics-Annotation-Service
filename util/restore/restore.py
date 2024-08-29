# restore.py
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
# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers
import botocore.exceptions as exceptions 
from botocore.exceptions import ClientError


# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('restore_config.ini')

# Add utility code here



dynamodb = boto3.client('dynamodb')
sqs = boto3.client('sqs')
glacier = boto3.client('glacier')

def main(): 
    while True: 
        response = sqs.receive_message(
            QueueUrl=config['aws']['SQSRestoreQueueUrl'], 
            MaxNumberOfMessages=1, 
            WaitTimeSeconds=20)
        try:  # Receive message
            message = response['Messages'][0]
            message_body = message['Body']
            receipt_handle = message['ReceiptHandle']
        except KeyError: # No messages in queue
            continue
        
        fixed_json_string = message_body.replace("'", '"')
        message_body_dict = json.loads(fixed_json_string)
        try: 
            user_id = message_body_dict['user_id']
        except KeyError: 
            print(f'KeyError!')
            continue
        _, _, _, _, role, _, _ = helpers.get_user_profile(id=user_id)
        if role == 'premium_user': 
            archive_ids = get_archive_ids(user_id)
            for item in archive_ids: 
                id = item['results_file_archive_id']['S']
                initiate_job(id=id)
        elif role == 'free_user': 
            pass
        else: 
            print(f'Error: 500, non-standard role value')

        response = sqs.delete_message(
                QueueUrl=config['aws']['AWS_SQS_RESTORE_QUEUE_URL'], 
                ReceiptHandle=receipt_handle)
 

def get_archive_ids(user_id): 
    try: 
        response = dynamodb.query(
            TableName=config['aws']['DynamodbName'], 
            IndexName=config['aws']['DynamoDBIndex'],
            Select='SPECIFIC_ATTRIBUTES', 
            ProjectionExpression="results_file_archive_id", 
            KeyConditionExpression="user_id = :u", 
            ExpressionAttributeValues={
                ":u": {"S": user_id}}, 
            FilterExpression='attribute_not_exists(s3_key_result_file) and attribute_exists(results_file_archive_id)' 
        )
        return response['Items']
    except ClientError as e: 
        print(e)
        return []

def initiate_job(id, tier='Expedited'): 
    try: 
        response = glacier.initiate_job( # Initiate job to retrieve from glacier
            vaultName=config['aws']['S3GlacierVaulttName'], 
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': id, 
                'SNSTopic': config['aws']['SNSThawTopicARN'], 
                'Tier': tier
            }
        )
        print(f"    initiating {tier} job with jobId {response['jobId']}\n       ArchivalId: {id}")
    except glacier.exceptions.InsufficientCapacityException: 
        initiate_job(id, tier='Standard')        


if __name__ == '__main__': 
    main()


### EOF
from flask import Flask, request, jsonify
import uuid
import subprocess
import os
import json
import boto3

from botocore.exceptions import ClientError


import sys

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('ann_config.ini')

def request_annotation():

    # Extract job parameters from the request body (NOT the URL query string!)
    # try:

        sqs = boto3.resource('sqs', region_name='us-east-1')
        sqs_client = boto3.client('sqs',region_name='us-east-1')

        queue_name = config['ANN']['QueueName']
        # Get the queue
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        queue_url = config['ANN']['QueueURL']
        # Receive messages 
        while True:
            response = sqs_client.receive_message(
                QueueUrl=queue_url, 
                AttributeNames=['All'], 
                MaxNumberOfMessages=1, 
                WaitTimeSeconds=20, VisibilityTimeout=30
            )
            try: 
                message = response['Messages'][0]
                message_body = message['Body']
                receipt_handle = message['ReceiptHandle']
            except KeyError: # No messages in queue
                print({'code': 400, 'status': 'error', 'message': 'Empty Queue'})
                continue
            message_dict = json.loads(json.loads(message_body)['Message'])
          
            bucket_name = message_dict.get('s3_inputs_bucket')
            job_id = message_dict.get('job_id')
            user_id = message_dict.get('user_id')
            user_email = message_dict.get('user_email')
            input_file_name = message_dict.get('input_file_name')
            s3_key_input_file = message_dict.get('s3_key_input_file')
            key = message_dict.get('key')
            if not bucket_name or not key:
                print({'code': 400, 'status': 'error', 'message': 'Missing bucket name or key'})
            
            # Get the input file S3 object and copy it to a local file
            home_dir = os.path.expanduser('~/mpcs-cc/gas/ann/')
            data_dir = os.path.join(home_dir,'download')
            os.makedirs(data_dir, exist_ok=True)  # Ensure the directory exists
            local_file_path = os.path.join(data_dir, input_file_name)

            s3 = boto3.client('s3')
            s3.download_file(bucket_name, key, local_file_path)

            dynamodb = boto3.resource('dynamodb')
            dynamobName = config['AWS']['DynamodbName']
            table = dynamodb.Table(dynamobName)

            try:
                response = table.update_item(
                    Key={
                    'job_id': job_id,
                    },
                    UpdateExpression='SET job_status = :status',
                    ExpressionAttributeValues={
                        ':status': 'RUNNING',
                        ':pending': 'PENDING'
                    },
                    ConditionExpression='job_status = :pending',  # Ensure the current status is PENDING
                    ReturnValues="UPDATED_NEW"
                        )
            except ClientError as e:
                if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                    print("Job status update condition not met (not PENDING).")
                    # Handle the condition not met case, e.g., log, raise an exception, etc.
                else:
                    # Handle other possible exceptions
                    print(e.response['Error']['Message'])
            
             # Launch annotation job as a background process
            try:
                ann_process = subprocess.Popen(['python', 'run.py', 'download/'+input_file_name, job_id, user_email,user_id])
                response = sqs_client.delete_message(
                    QueueUrl=queue_url, 
                    ReceiptHandle=receipt_handle)

            except Exception as e:
                print(e)
                print ({'code': 500, 'status': 'error', 'message': str(e)})



                    



# @app.route('/annotations/<job_id>', methods=['GET'])
# def get_job(job_id):
#   try:
#       home_dir = os.path.expanduser('~/mpcs-cc')
#       job_dir = os.path.join(home_dir, 'jobs')
#       job_file_path = os.path.join(job_dir, f"{job_id}.job")

#       if not os.path.exists(job_file_path):
#           return jsonify({'code': 404, 'status': 'error', 'message': 'Job not found'}), 404

#       with open(job_file_path, 'r') as file:
#           job_info = file.read()

#       input_file = jobs.get(job_id)
#       if not input_file:
#           return jsonify({'code': 404, 'status': 'error', 'message': 'Input file not found'}), 404

#       log_file_path = os.path.join(home_dir, 'anntools', 'data', f"{input_file}.count.log")

#       if os.path.exists(log_file_path):
#           with open(log_file_path, 'r') as log_file:
#             log_content = log_file.read()
#           with open(job_file_path, 'w') as file:
#             file.write(f"Input file: {input_file}\nStatus: Completed")
#           return jsonify({'code': 200, 'data': {'job_id': job_id, 'job_status': 'completed', 'log': log_content}}), 200
#       else:
#           return jsonify({'code': 200, 'data': {'job_id': job_id, 'job_status': 'running'}}), 200
#   except Exception as e:
#         return jsonify({'code': 500, 'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__': 
    print('... Receiving from queue ...')
    request_annotation()
import sys
import time
import driver
import boto3
from botocore.exceptions import ClientError
import os
import sys
import json

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('ann_config.ini')

sys.path.insert(1, config.get('SYSTEM', 'HelpersModuleFilePath'))
import helpers

"""A rudimentary timer for coarse-grained profiling"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

def upload_file_to_s3(file_path, bucket, user_id):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    """
    object_name = config['AWS']['AWS_S3_KEY_PREFIX']+ user_id+"/"+ file_path.split('/')[-1]

    s3_client = boto3.client('s3', region_name = 'us-east-1')
    try:
        response = s3_client.upload_file(file_path, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def cleanup_local_file(file_name):
    """Delete a local file"""
    os.remove(file_name)

if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        input_file_name = sys.argv[1]

        job_id = sys.argv[2]
        user_email = sys.argv[3]
        user_id = sys.argv[4]
        with Timer():
            driver.run(input_file_name, 'vcf')
        
        # Define S3 bucket name for results
        bucket_name = config['AWS']['BucketName']
        
        # Upload the results file and log file to S3 results bucket
        home_dir = os.path.expanduser('~/mpcs-cc/gas/ann/')
        file_name_prefix = input_file_name.split('.')[0]


        results_file =  os.path.join(home_dir, file_name_prefix+'.annot.vcf')
        log_file = os.path.join(home_dir, input_file_name+ '.count.log')


        # Assume upload_file_to_s3 modifies these keys as needed
        upload_file_to_s3(results_file, bucket_name, user_id)
        upload_file_to_s3(log_file, bucket_name, user_id)

        dynamodb = boto3.resource('dynamodb')
        dynamobName = config['AWS']['DynamodbName']
        table = dynamodb.Table(dynamobName)
        timestamp = int(time.time())
        s3_key_prefix = config['AWS']['AWS_S3_KEY_PREFIX']
        # user_id = session['primary_identity']
       

        try:
            response = table.update_item(
            Key={
                    'job_id': job_id,
                } ,
            UpdateExpression='SET s3_key_input_file = :s3_input, s3_key_result_file = :s3_result, job_status = :status, complete_time = :complete, s3_key_log_file = :s3_log',
            ExpressionAttributeValues={
                ':s3_input': s3_key_prefix + user_id+ '/' + input_file_name.split('/')[1],
                ':s3_result': s3_key_prefix + user_id+ '/'+results_file.split('/')[-1],
                ':status': 'COMPLETED',
                ':complete': timestamp,
                ':s3_log': s3_key_prefix + user_id+ '/'+log_file.split('/')[-1]
            },
            ReturnValues="UPDATED_NEW"
        )

        except ClientError as e:
            logging.error(e)
            print("Error updating DynamoDB item.")
            sys.exit(1)  # Exits the script with an error code of 1, indicating failure.

        sns_client = boto3.client('sns', region_name = 'us-east-1')
        message = json.dumps({"default": json.dumps({"job_id": job_id, "email":user_email})})  
        topic_arn = config['AWS']['SNS_Result_ARN']
        try:
            response = sns_client.publish(
                TopicArn=topic_arn,
                Message=message,
                MessageStructure='json',
                MessageGroupId = 'resultNotification',
                MessageDeduplicationId = 'resultNotification',
            )
            print(response)
            print(f"Message published successfully. Message ID: {response['MessageId']}")
        except boto3.exceptions.Boto3Error as e:
            print(f"An error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        _, _, _, _, role, _, _ = helpers.get_user_profile(id=user_id) # get user role, if free_user, send message to queue
        print(role)
        if role == 'free_user': 
            sqs = boto3.resource('sqs')
            archive_queue_name = config['AWS']['Archive_Queue_Name']
            queue = sqs.get_queue_by_name(QueueName = archive_queue_name)
            queue.send_message(
                MessageBody=str({
                    'user_id': user_id, 
                    'job_id': job_id, 
                    's3_key_result_file': s3_key_prefix + user_id+ '/'+results_file.split('/')[-1]})
            )
            print("send archive message sucessfully")
        else: 
            pass





        # Cleanup local files
        cleanup_local_file(results_file)
        cleanup_local_file(log_file)
        cleanup_local_file(input_file_name)
        
        print(f"Results and log files for {input_file_name} have been uploaded to S3 and local copies deleted.")
        pass
    else:
        print("A valid .vcf file must be provided as input to this program.")
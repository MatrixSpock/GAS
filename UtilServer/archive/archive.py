# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3
import json
import time
import os
from botocore.exceptions import ClientError, NoCredentialsError
from configparser import ConfigParser

# AWS Configuration
aws_region = 'us-east-1'

# Load configuration
config = ConfigParser()
config.read('archive_config.ini')

# DynamoDB Configuration
dynamodb_table_name = config.get('dynamodb', 'table_name')

# SQS Configuration
sqs_queue_url = config.get('sqs', 'queue_url')

# Glacier Configuration
glacier_vault_arn = config.get('glacier', 'vault_arn')

# Initialize AWS clients
s3 = boto3.client('s3', region_name=aws_region)
glacier = boto3.client('glacier', region_name=aws_region)
dynamodb = boto3.resource('dynamodb', region_name=aws_region)
sqs = boto3.client('sqs', region_name=aws_region)

# DynamoDB Table
table = dynamodb.Table(dynamodb_table_name)

def poll_sqs():
    """Poll the SQS queue for messages and process them."""
    while True:
        response = sqs.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            MessageAttributeNames=['All']
        )

        if 'Messages' in response:
            for message in response['Messages']:
                print("Received message:", message['Body'])
                message_body = json.loads(message['Body'])

                # Print the message body to debug the structure
                print("Message Body:", message_body)

                # Extract the actual message which contains job details
                if 'Message' not in message_body:
                    print("Error: 'Message' key not found in message body. Skipping message.")
                    continue  # Skip this message

                # Extract and parse the nested message
                actual_message = json.loads(message_body['Message'])

                if 'job_id' not in actual_message:
                    print("Error: 'job_id' key not found in actual message. Skipping message.")
                    continue  # Skip this message

                job_id = actual_message['job_id']

                # Get user_id and user_role from DynamoDB using job_id
                response = table.get_item(Key={'job_id': job_id})
                if 'Item' in response:
                    user_id = response['Item']['user_id']
                    user_role = response['Item']['user_role']
                    
                    if user_role == 'free_user':
                        print(f"User {user_id} is a free user. Waiting for 5 minutes before archiving.")
                        time.sleep(5)  # Wait for 5 minutes

                        # Get the S3 key for the annotated file
                        s3_bucket = actual_message['s3_results_bucket']
                        s3_key = actual_message['s3_key_result_file']

                        # Archive the file to Glacier and update DynamoDB
                        archive_id = archive_file(job_id, s3_bucket, s3_key)
                        
                        # After successful archiving, delete the message from the queue
                        sqs.delete_message(
                            QueueUrl=sqs_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                    else:
                        print(f"User {user_id} is a premium user. Deleting message.")
                        # Delete message from the queue immediately for premium users
                        sqs.delete_message(
                            QueueUrl=sqs_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                else:
                    print(f"Job ID {job_id} not found in DynamoDB.")
        else:
            print("No messages in queue. Waiting...")

def archive_file(job_id, s3_bucket, s3_key):
    """Archive the annotated file to Glacier and update DynamoDB."""
    try:
        # Download the annotated file from S3
        download_path = f'/tmp/{os.path.basename(s3_key)}'
        s3.download_file(s3_bucket, s3_key, download_path)

        # Upload the file to Glacier
        with open(download_path, 'rb') as file:
            response = glacier.upload_archive(vaultName='mpcs-cc', body=file)
            archive_id = response['archiveId']

        # Update DynamoDB with the Glacier archive ID
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET results_file_archive_id = :archive_id',
            ExpressionAttributeValues={':archive_id': archive_id}
        )

        # Delete the file from S3
        s3.delete_object(Bucket=s3_bucket, Key=s3_key)
        print(f"Successfully archived {s3_key} to Glacier and updated DynamoDB")
        
        return archive_id
    except (NoCredentialsError, ClientError) as e:
        print(f"Error during archiving: {e}")
        raise e

if __name__ == '__main__':
    poll_sqs()


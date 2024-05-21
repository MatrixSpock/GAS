# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3
import json
import os
import sys
from botocore.exceptions import ClientError, NoCredentialsError
from configparser import ConfigParser

# AWS Configuration
aws_region = 'us-east-1'

# Load configuration
config = ConfigParser()
config.read('thaw_config.ini')

# SQS Configuration
sqs_queue_url = config.get('sqs', 'queue_url')

# Initialize AWS clients
sqs = boto3.client('sqs', region_name=aws_region)
glacier = boto3.client('glacier', region_name=aws_region)
s3 = boto3.client('s3', region_name=aws_region)

def poll_sqs():
    """Poll the SQS queue for job completion messages and process them."""
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
                try:
                    actual_message = json.loads(message_body['Message'])

                    # Check if the message is from Glacier
                    if 'jobId' in actual_message:
                        # Message from Glacier (job completion)
                        job_id = actual_message['jobId']
                        archive_id = actual_message['ArchiveId']
                        vault_name = actual_message['VaultName']

                        # Download the restored archive from Glacier
                        print(f"Retrieving restored archive for job_id: {job_id}")
                        job_output = glacier.get_job_output(
                            vaultName=vault_name,
                            jobId=job_id
                        )
                        archive_content = job_output['body'].read()

                        # Save the restored archive to S3
                        s3_key = f'restored/{archive_id}'
                        s3.put_object(
                            Bucket=config.get('s3', 'results_bucket'),
                            Key=s3_key,
                            Body=archive_content
                        )
                        print(f"Restored archive saved to S3 at key: {s3_key}")

                    # After processing, delete the message from the queue
                    sqs.delete_message(
                        QueueUrl=sqs_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    continue  # Skip this message
                except ClientError as e:
                    print(f"Error retrieving job output: {e}")
                    continue  # Skip this message

        else:
            print("No messages in queue. Waiting...")

if __name__ == '__main__':
    poll_sqs()



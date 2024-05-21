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

# SNS Message is a string 
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
                print("Received message:", json.dumps(message, indent=4))
                try:
                    message_body = json.loads(message['Body'])
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from message body: {e}")
                    continue  # Skip this message

                # Print the message body to debug the structure
                print("Message Body:", json.dumps(message_body, indent=4))

                # Extract the actual message which contains job details
                if 'Message' not in message_body:
                    print("Error: 'Message' key not found in message body. Skipping message.")
                    continue  # Skip this message

                # Extract and parse the nested message
                actual_message_str = message_body['Message']
                print("Actual Message String:", actual_message_str)
                
                # Check if the actual message is JSON
                try:
                    actual_message = json.loads(actual_message_str)
                    print(f"Check if the actual message is JSON: {actual_message}")
                except json.JSONDecodeError:
                    print(f"The actual message is not a JSON - actual_message_str: {actual_message_str}")
                    sqs.delete_message(
                        QueueUrl=sqs_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )   
                    continue  # Skip this message

                # Check if the message is from Glacier
                print("Check if the message is from Glacier")
                if 'jobId' in actual_message:
                    try:
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
                        print(f"Download the restored archive from Glacier - job_output: {job_output}")
                        archive_content = job_output['body'].read()

                        # Save the restored archive to S3
                        s3_key = f'restored/{archive_id}'
                        s3.put_object(
                            Bucket=config.get('s3', 'results_bucket'),
                            Key=s3_key,
                            Body=archive_content
                        )
                        print(f"Restored archive saved to S3 at key: {s3_key}")

                    except ClientError as e:
                        print(f"Error retrieving job output: {e}")
                        continue  # Skip this message
                else:
                    print(f"Message not from Glacier: {actual_message}")
                    sqs.delete_message(
                        QueueUrl=sqs_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )   

                print(f"After processing, delete the message from the queue")
                print(f"SQS Queue URL: {sqs_queue_url}")
                sqs.delete_message(
                    QueueUrl=sqs_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )

        else:
            print("No messages in queue. Waiting...")

if __name__ == '__main__':
    poll_sqs()
# restore.py
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
import time
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError, NoCredentialsError
from configparser import ConfigParser

# Load configuration
config = ConfigParser()
config.read('restore_config.ini')


# AWS Configuration
aws_region = config.get('aws', 'aws_region_name')
aws_account_id = config.get('aws', 'account_id')

# Glacier Configuration
vault_name = config.get('glacier', 'vault_name')


# SQS Configuration
sqs_restore_queue_url = config.get('sqs', 'restore_queue_url')
thaw_completion_topic_arn = config.get('sns', 'restore_completion_topic_arn')

# Initialize AWS clients
sqs = boto3.client('sqs', region_name=aws_region)
glacier_client = boto3.client('glacier', region_name=aws_region)
dynamodb = boto3.resource('dynamodb', region_name=aws_region)
table = dynamodb.Table('jcorning_annotations')

def poll_sqs():
    """Poll the SQS queue for messages and process them."""
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=sqs_restore_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                MessageAttributeNames=['All']
            )
        except ClientError as e:
            print(f"Error receiving messages from SQS: {e}")
            time.sleep(60)
            continue

        if 'Messages' in response:
            for message in response['Messages']:
                print("Received message:", message['Body'])
                try:
                    message_body = json.loads(message['Body'])
                except json.JSONDecodeError as e:
                    print(f"Error decoding message body: {e}")
                    continue

                if 'Message' not in message_body:
                    print("Error: 'Message' key not found in message body. Skipping message.")
                    continue

                try:
                    actual_message = json.loads(message_body['Message'])
                except json.JSONDecodeError as e:
                    print(f"Error decoding actual message: {e}")
                    continue

                user_id = actual_message.get('user_id')
                if not user_id:
                    print("Error: 'user_id' key not found in actual message. Skipping message.")
                    continue

                # Process the restoration request for the user_id
                restore_user_data(user_id)

                # Delete the message from the queue
                try:
                    sqs.delete_message(
                        QueueUrl=sqs_restore_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except ClientError as e:
                    print(f"Error deleting message from SQS: {e}")
        else:
            print("No messages in queue. Waiting...")

def restore_user_data(user_id):
    """Function to handle the restoration process for a user_id."""
    print(f"Restoring data for user_id: {user_id}")
    
    try:
        response = table.scan(
            FilterExpression=Key('user_id').eq(user_id)
        )
    except ClientError as e:
        print(f"Error scanning DynamoDB: {e}")
        return

    if 'Items' not in response or not response['Items']:
        print(f"No archived objects found for user_id: {user_id}")
        return

    archived_items = response['Items']

    for item in archived_items:
        if 'results_file_archive_id' in item:
            initiate_glacier_retrieval(item)
        else:
            print(f"No results_file_archive_id found for item with user_id: {user_id}")

def initiate_glacier_retrieval(item):
    archive_id = item['results_file_archive_id']
    job_id = item['job_id']
    try:
        print(f"Initiating expedited retrieval for archive_id: {archive_id}")
        response = glacier_client.initiate_job(
            accountId=aws_account_id,
            vaultName=vault_name,
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Description': 'Expedited retrieval',
                'Tier': 'Expedited',
                'SNSTopic': thaw_completion_topic_arn
            }
        )
        
        glacier_restore_job_id = response['jobId']
        print(f"Initiated expedited retrieval job with ID: {glacier_restore_job_id}")
        
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET glacier_restore_job_id = :glacier_restore_job_id',
            ExpressionAttributeValues={':glacier_restore_job_id': glacier_restore_job_id}
        )
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'InsufficientCapacityException':
            print(f"Expedited retrieval failed for archive_id: {archive_id}. Initiating standard retrieval.")
            try:
                response = glacier_client.initiate_job(
                    accountId=aws_account_id,
                    vaultName=vault_name,
                    jobParameters={
                        'Type': 'archive-retrieval',
                        'ArchiveId': archive_id,
                        'Description': 'Standard retrieval',
                        'Tier': 'Standard',
                        'SNSTopic': thaw_completion_topic_arn
                    }
                )
                glacier_restore_job_id = response['jobId']
                print(f"Initiated standard retrieval job with ID: {glacier_restore_job_id}")
                
                table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression='SET glacier_restore_job_id = :glacier_restore_job_id',
                    ExpressionAttributeValues={':glacier_restore_job_id': glacier_restore_job_id}
                )
            except ClientError as e:
                print(f"Standard retrieval failed for archive_id: {archive_id}. Error: {e}")
        else:
            print(f"Error initiating expedited retrieval for archive_id: {archive_id}. Error: {e}")

if __name__ == '__main__':
    poll_sqs()
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
import tempfile
from botocore.exceptions import ClientError, NoCredentialsError
from configparser import ConfigParser


# Load configuration
config = ConfigParser()
config.read('thaw_config.ini')

# AWS Configuration
aws_region = config.get('aws', 'aws_region_name')
aws_account_id = config.get('aws', 'account_id')
results_bucket = config.get('aws', 'results_bucket')

# Glacier Configuration
vault_name = config.get('glacier', 'vault_name')

# SQS Configuration
thaw_queue_url = config.get('sqs', 'glacier_thaw_queue_url')

# Initialize AWS clients
sqs = boto3.client('sqs', region_name=aws_region)
glacier = boto3.client('glacier', region_name=aws_region)
s3 = boto3.client('s3', region_name=aws_region)
dynamodb = boto3.resource('dynamodb', region_name=aws_region)

#DynamoDB
table = dynamodb.Table('jcorning_annotations')


def get_s3_key_result_file(archive_id):
    """Retrieve the s3_key_result_file from DynamoDB using the archive_id."""
    try:
        response = table.query(
            IndexName='results_file_archive_id_index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('results_file_archive_id').eq(archive_id)
        )

        if 'Items' in response and len(response['Items']) > 0:
            item = response['Items'][0]
            print(f"item: {item}")
            return item
        else:
            print(f"No matching item found for archive_id: {archive_id}")
            return None

    except ClientError as e:
        print(f"Error querying DynamoDB: {e}")
        return None
    
def poll_sqs():
    """Poll the SQS queue for job completion messages and process them."""
    while True:
        response = sqs.receive_message(
            QueueUrl=thaw_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            MessageAttributeNames=['All']
        )

        if 'Messages' in response:
            print(f"SQS Response: {response}")
            for message in response['Messages']:
                try:
                    print(f"message: {message}")
                    # Parse the SNS message
                    sns_message = json.loads(message['Body'])
                    print(f"sns_message: {sns_message}")
                    sns_message_body = json.loads(sns_message['Message'])
                    print(f"sns_message_body: {sns_message_body}")


                    glacier_job_id = sns_message_body['JobId']
                    archive_id = sns_message_body['ArchiveId']

                    print(f"Retrieving restored archive for glacier_job_id: {glacier_job_id}")
                    job_output = glacier.get_job_output(
                        vaultName=vault_name,
                        accountId=aws_account_id,
                        jobId=glacier_job_id
                    )

                    # Get the s3_key_result_file from DynamoDB
                    item = get_s3_key_result_file(archive_id)
                    if item is None:
                        continue  # Skip this message if no matching item found

                    s3_key_result_file = item['s3_key_result_file']

                    # Save the restored archive to a temporary file
                    with tempfile.TemporaryDirectory() as temp_dir:
                        file_path = os.path.join(temp_dir, os.path.basename(s3_key_result_file))
                        with open(file_path, 'wb') as file:
                            file.write(job_output['body'].read())
                        print(f"Restored archive saved to temporary file: {file_path}")

                        # Upload the file to the specified S3 bucket
                        s3.upload_file(file_path, results_bucket, s3_key_result_file)
                        print(f"File uploaded to S3 bucket: {results_bucket}, key: {s3_key_result_file}")

                        # Delete the file from Glacier
                        glacier.delete_archive(
                            vaultName=vault_name,
                            archiveId=archive_id
                        )
                        print(f"Deleted archive from Glacier vault: {vault_name}, archive ID: {archive_id}")

                        # Update the DynamoDB item to remove results_file_archive_id and glacier_restore_job_id
                        table.update_item(
                            Key={'job_id': item['job_id']},
                            UpdateExpression="REMOVE results_file_archive_id, glacier_restore_job_id"
                        )
                        print(f"Updated DynamoDB item to remove archive ID and restore job ID for job_id: {item['job_id']}")

                    # Delete the processed message from the SQS queue
                    sqs.delete_message( 
                        QueueUrl=thaw_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )

                except ClientError as e:
                    print(f"Error retrieving job output: {e}")
                    continue  # Skip this message

        else:
            print("No messages in queue. Waiting...")

if __name__ == '__main__':
    poll_sqs()
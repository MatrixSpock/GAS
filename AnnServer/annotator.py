import boto3
import subprocess
import os
import configparser
import json
import logging
from botocore.exceptions import ClientError

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load AWS credentials and other configurations from the config file
config = configparser.ConfigParser()
config_file_path = '/home/ec2-user/mpcs-cc/config/ann_config.ini'
if not os.path.exists(config_file_path):
    logging.error(f"Config file not found: {config_file_path}")
else:
    config.read(config_file_path)

aws_access_key_id = config.get('aws', 'aws_access_key_id')
aws_secret_access_key = config.get('aws', 'aws_secret_access_key')
aws_region_name = config.get('aws', 'aws_region_name')

results_bucket = config.get('s3', 'results_bucket')
results_key_prefix = config.get('s3', 'results_key_prefix')
inputs_bucket = config.get('s3', 'inputs_bucket')
input_key_prefix = config.get('s3', 'input_key_prefix')

table_name = config.get('dynamodb', 'table_name')

queue_url = config.get('sqs', 'queue_url')

# Log configuration values to verify they are read correctly
logging.info(f"AWS Region: {aws_region_name}")
logging.info(f"Results Bucket: {results_bucket}")
logging.info(f"Results Key Prefix: {results_key_prefix}")
logging.info(f"Inputs Bucket: {inputs_bucket}")
logging.info(f"Input Key Prefix: {input_key_prefix}")
logging.info(f"DynamoDB Table Name: {table_name}")
logging.info(f"SQS Queue URL: {queue_url}")

# Initialize AWS resources
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region_name
)
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region_name
)
table = dynamodb.Table(table_name)
sqs = boto3.client('sqs', region_name=aws_region_name)

def process_annotation(job_details):
    try:
        job_id = job_details['job_id']
        input_file_name = job_details['s3_key_input_file']

        # Ensure the input file key is correct
        if input_file_name.startswith(input_key_prefix):
            s3_key_input_file = input_file_name
        else:
            s3_key_input_file = input_key_prefix + input_file_name

        logging.info(f"Downloading file from S3: Bucket={inputs_bucket}, Key={s3_key_input_file}")

        # Download file from S3
        local_file_path = f"/home/ec2-user/s3_downloads/{job_id}.vcf"
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        s3_client.download_file(inputs_bucket, s3_key_input_file, local_file_path)

        # Update DynamoDB job status to RUNNING
        update_job_status(job_id, 'RUNNING')

        # Run the annotation process using subprocess.Popen
        process = subprocess.Popen(['python', './anntools/run.py', local_file_path, job_id])
        process.wait()  # Wait for the process to complete

        if process.returncode == 0:
            # Update DynamoDB job status to COMPLETED
            update_job_status(job_id, 'COMPLETED')
            return True
        else:
            logging.error("Annotation process failed.")
            return False
    except KeyError as e:
        logging.error(f"Missing key {str(e)} in job details: {job_details}")
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logging.error(f"The specified key does not exist: {s3_key_input_file}")
        else:
            logging.error(f"An error occurred: {e}")
        return False
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return False

def update_job_status(job_id, status):
    table.update_item(
        Key={'job_id': job_id},
        UpdateExpression='SET job_status = :s',
        ExpressionAttributeValues={
            ':s': status
        }
    )

def main():
    while True:
        logging.info("Polling for messages...")
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20  # Long polling
        )

        messages = response.get('Messages', [])
        if messages:
            message = messages[0]
            receipt_handle = message['ReceiptHandle']
            message_body = json.loads(message['Body'])
            logging.info(f"Received message: {message_body}")

            # Check for nested SNS message
            if 'Message' in message_body:
                sns_message = json.loads(message_body['Message'])
                logging.info(f"Extracted SNS message content: {sns_message}")
                if process_annotation(sns_message):
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    logging.info("Message processed successfully and deleted.")
                else:
                    logging.error("Failed to process message.")
            else:
                logging.error("No 'Message' key in received message body.")
        else:
            logging.info("No new messages.")

if __name__ == '__main__':
    main()

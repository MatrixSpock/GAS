import json
import boto3
from botocore.exceptions import ClientError

# Replace with your name and CNET ID
SENDER = "jcorning@mpcs-cc.com"
REGION = "us-east-1"


def lambda_handler(event, context):
    client = boto3.client("ses", region_name=REGION)

    for record in event['Records']:
        try:
            # Process each record (message) from the SQS queue
            message_body = json.loads(record['body'])
            print("Message Body:", json.dumps(message_body, indent=4))
            
            # Extract the actual message
            if 'Message' in message_body:
                actual_message = json.loads(message_body['Message'])
                print("Actual Message:", json.dumps(actual_message, indent=4))

                # Extract relevant information from the message
                job_status = actual_message.get('StatusCode')
                job_id = actual_message.get('JobId')
                vault_name = actual_message.get('VaultName')
                archive_id = actual_message.get('ArchiveId')
                
                recipient = 'jcorning@uchicago.edu' 
                subject = f"Glacier Job {job_id} Status Update"
                body_text = f"Your Glacier job with ID {job_id} has the following status: {job_status}."
                body_html = f"""<html>
                <head></head>
                <body>
                  <h1>Glacier Job Status Update</h1>
                  <p>Your Glacier job with ID {job_id} in vault {vault_name} has the following status: {job_status}.</p>
                  <p>Archive ID: {archive_id}</p>
                </body>
                </html>"""

                # Send the email
                response = client.send_email(
                    Destination={
                        'ToAddresses': [recipient],
                    },
                    Message={
                        'Body': {
                            'Html': {
                                'Charset': "UTF-8",
                                'Data': body_html,
                            },
                            'Text': {
                                'Charset': "UTF-8",
                                'Data': body_text,
                            },
                        },
                        'Subject': {
                            'Charset': "UTF-8",
                            'Data': subject,
                        },
                    },
                    Source=SENDER,
                )
                
                print(f"Email sent to {recipient} for job {job_id}")
            else:
                print("No 'Message' key in the message body.")
        
        except ClientError as error:
            print(f"Error sending email: {error.response['Error']['Message']}")
            return {
                'statusCode': 500,
                'body': json.dumps(error.response)
            }
        except json.JSONDecodeError as json_error:
            print(f"Error decoding JSON: {json_error}")
            continue  # Skip to the next record

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully!')
    }
# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from botocore.config import Config

from flask import (abort, flash, redirect, render_template,
  request, session, url_for, jsonify)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile

import configparser

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=BotoConfig(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']

  # Authenticate users and use their profile information
  user_id = session.get('primary_identity')
  profile = get_profile(identity_id=user_id)

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))

  # Extract the job ID from the S3 key
  job_id = s3_key.split('/')[-1].split('~')[0]
  user_id = session.get('primary_identity')
  profile = get_profile(identity_id=user_id)

  # Persist job to database
  dynamo = boto3.resource('dynamodb')
  table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  timestamp = int(time.time())

  item = {
        "job_id": job_id,
        "user_id": user_id, 
        "user_name": profile.name,
        "user_email": profile.email,
        "input_file_name": s3_key,
        "s3_inputs_bucket": bucket_name,
        "s3_key_input_file": s3_key,
        "submit_time": timestamp,
        "job_status": "PENDING"
  }

  table.put_item(Item=item)

  # Send message to request queue
  sns = boto3.client('sns')
  sns.publish(
      TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
      Message=json.dumps(item)
  )


  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated

def annotations_list():
    user_id = session.get('primary_identity')

    # Create a DynamoDB resource and reference the Annotations table
    dynamo_db = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamo_db.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

    try:
        response = table.query(
            IndexName='user_id_index',  # Replace with the actual index name if different
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        annotations = response['Items']

        # Convert epoch time to human-readable format
        for annotation in annotations:
            if 'submit_time' in annotation:
                annotation['submit_time'] = datetime.fromtimestamp(int(annotation['submit_time'])).strftime('%Y-%m-%d %H:%M:%S')

    except ClientError as e:
        app.logger.error(f"Failed to query DynamoDB: {e}")
        return abort(500)

    return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    user_id = session.get('primary_identity')
    dynamo_db = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamo_db.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

    # Retrieve the job by ID
    try:
        response = table.get_item(Key={'job_id': id})
    except ClientError as e:
        app.logger.error(f"Unable to retrieve job: {e}")
        return abort(500)

    if 'Item' not in response:
        return abort(404, description="Job not found")

    job = response['Item']

    # Verify the job belongs to the user
    if job['user_id'] != user_id:
        return abort(403, description="Not authorized to view this job")

    # Convert epoch times to human-readable format
    job['submit_time'] = datetime.fromtimestamp(job['submit_time']).strftime('%Y-%m-%d %H:%M:%S')
    if 'complete_time' in job:
        job['completec_time'] = '12/12/2024' # datetime.fromtimestamp(job['complete_time']).strftime('%Y-%m-%d %H:%M:%S')

    # Generate URLs for input and result files
    s3_client = boto3.client('s3')
    job['input_file_url'] = s3_client.generate_presigned_url('get_object', Params={
        'Bucket': job['s3_inputs_bucket'],
        'Key': job['s3_key_input_file'],
    }, ExpiresIn=3600) 
    
    if job['job_status'] == 'COMPLETED':
        job['result_file_url'] = s3_client.generate_presigned_url('get_object', Params={
            'Bucket': job['s3_results_bucket'],
            'Key': job['s3_key_result_file'],
        }, ExpiresIn=3600)

    return render_template('annotation_details.html', annotation=job)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    user_id = session.get('primary_identity')
    dynamo_db = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamo_db.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

    try:
        response = table.get_item(Key={'job_id': id})
    except ClientError as e:
        app.logger.error(f"Unable to retrieve job: {e}")
        return abort(500)

    if 'Item' not in response:
        return abort(404, description="Job not found")

    job = response['Item']

    if job['user_id'] != user_id:
        return abort(403, description="Not authorized to view this job")

    # Check if log file exists and generate presigned URL to display log file content 
    if 'log_file' in job:
        log_file_key = job['log_file']
        s3 = boto3.client('s3')
        log_file_url = s3.generate_presigned_url('get_object', Params={
            'Bucket': job['s3_results_bucket'],
            'Key': log_file_key,
        }, ExpiresIn=3600)
        log_file_contents = requests.get(log_file_url).text
    else:
        log_file_contents = "Log file is not available."

    return render_template('view_log.html', job_id=id, log_file_contents=log_file_contents)


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF

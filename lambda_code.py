import json
import boto3
import os
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")

# Define the table name
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")

def lambda_handler(event, context):
    # Log the full event for debugging
    # print("Received event:", json.dumps(event))
    
    # Check if 'body' exists in the event
    if "body" not in event or not event["body"]:
        return {
            "statusCode": 400,
            "body": "Request body is missing."
        }

    # Parse the body
    try:
        payload = json.loads(event["body"]) 
        subject = payload.get("subject", "Default Subject")
        message = payload.get("message", "Default Message")
        protocol = payload.get("protocol", "SMS")
    except (KeyError, TypeError, json.JSONDecodeError) as e:
        return {
            "statusCode": 400,
            "body": f"Invalid JSON format: {str(e)}"
        }

    # Get SNS topic ARN from environment variable
    email_sns_topic_arn = os.getenv("EMAIL_SNS_TOPIC_ARN")
    sms_sns_topic_arn = os.getenv("SMS_SNS_TOPIC_ARN")
    push_sns_topic_arn = os.getenv("PUSH_SNS_TOPIC_ARN")
    if not email_sns_topic_arn or not sms_sns_topic_arn or not push_sns_topic_arn:
        return {
            "statusCode": 500,
            "body": "SNS_TOPIC_ARN environment variable not set."
        }

    # Publish message to SNS
    try:
        if protocol == 'EMAIL':
            sns_client.publish(
                TopicArn=email_sns_topic_arn,
                Message=message,
                Subject=subject
            )
            response_message = f"Notification successfully sent to SNS topic: {email_sns_topic_arn}"

        elif protocol == 'SMS':
            sns_client.publish(
                TopicArn=sms_sns_topic_arn,
                Message=message,
                Subject=subject
            )
            response_message = f"Notification successfully sent to SNS topic: {sms_sns_topic_arn}"

        elif protocol == 'PUSH':
            sns_client.publish(
                TopicArn=push_sns_topic_arn,
                Message=message,
                Subject=subject
            )
            response_message = f"Notification successfully sent to SNS topic: {push_sns_topic_arn}"

        else:
            return {
                "statusCode": 400,
                "body": "Invalid protocol. Must be EMAIL, SMS, or PUSH."
            }

        # Store the payload info in DynamoDB
        store_payload_in_dynamodb(protocol, subject, message)

        return {
            "statusCode": 200,
            "body": json.dumps(response_message)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error sending notification: {str(e)}"
        }

def store_payload_in_dynamodb(protocol, subject, message):
    # Get the DynamoDB table
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    
    # Increment the counter for the protocol
    try:
        # Update the counter and store the payload
        response = table.update_item(
            Key={'protocol': protocol},
            UpdateExpression="SET #count = if_not_exists(#count, :start) + :inc, subject = :subject, message = :message",
            ExpressionAttributeNames={
                "#count": "counter"
            },
            ExpressionAttributeValues={
                ":inc": 1,
                ":start": 0,
                ":subject": subject,
                ":message": message
            }
        )
    except Exception as e:
        print(f"Error storing payload in DynamoDB: {str(e)}")
        raise
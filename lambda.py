import json
import boto3
import logging
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def delete_message_from_queue(queue_url):
    sqs = boto3.client('sqs')
    try:
        # Receive a message from the queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )

        # Check if there is a message to delete
        messages = response.get('Messages', [])
        if messages:
            # Get the ReceiptHandle of the received message
            receipt_handle = messages[0]['ReceiptHandle']
            
            # Delete the received message
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            logger.info("Message deleted successfully.")
        else:
            logger.info("No messages in the queue to delete.")

    except Exception as e:
        # Log the exception
        logger.error(f"Error in deleting message from queue: {str(e)}")

def get_max_retries_from_parameter_store():
    ssm = boto3.client('ssm')
    parameter_name = '/your/parameter/store/max_retries'
    response = ssm.get_parameter(Name=parameter_name, WithDecryption=False)
    return int(response['Parameter']['Value'])

def get_dlq_url():
    # Provide the DLQ URL directly
    return 'https://sqs.us-east-1.amazonaws.com/958686716208/DLQ1'

def send_to_dlq(event):
    # Send the event to the Dead Letter Queue
    dlq = boto3.client('sqs')
    dlq_url = get_dlq_url()
    dlq.send_message(QueueUrl=dlq_url, MessageBody=json.dumps(event))

def send_event_to_sqs(event, retry_count):
    # Send the event back to the SQS queue with updated retry count
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/958686716208/primarysqs'
    
    try:
        response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(event))
        # Log the MessageId
        logger.info(f"Message sent to queue. MessageId: {response['MessageId']}")
        return True
    except Exception as e:
        # Log the exception
        logger.error(f"Error in sending message to queue: {str(e)}")
        raise e  # Raising exception for handling retry logic in lambda_handler

# Lambda handler function
def lambda_handler(event, context):
    try:
        # Check if the event has the expected structure for CustomEvent
        if 'detail-type' in event and event.get('detail-type') == 'CustomEvent':
            # Your processing logic here if the event type is correct
            logger.info("Event processed successfully!")
        else:
            # Raise an exception if the event type is not as expected
            raise ValueError(f"Unexpected event type: {event.get('detail-type', '')}")

        return {
            'statusCode': 200,
            'body': json.dumps('Process completed successfully!')
        }

    except Exception as e:
        # Log the exception
        logger.error(f"Error: {str(e)}")

        # Check if retries are within the limit
        max_retries = get_max_retries_from_parameter_store()
        retry_count = event.get('detail', {}).get('retry_count', 0) + 1

        if retry_count >= max_retries:
            # If max retries exceeded, move to Dead Letter Queue
            dlq_url = get_dlq_url()
            sqs = boto3.client('sqs')
            sqs.send_message(QueueUrl=dlq_url, MessageBody=json.dumps(event))

            return {
                'statusCode': 500,
                'body': json.dumps(f'Error: {str(e)}. Moved to Dead Letter Queue.')
            }
        else:
            # Retry logic
            try:
                # Main logic - for testing, let's throw an exception
                raise Exception("Intentional exception for testing retries")

            except Exception as e:
                print(f"Retry {retry_count + 1} failed. Exception: {str(e)}")

                # Increment retry count
                retry_count += 1

                # Calculate sleep time based on retry count
                sleep_time = 30 * 2 ** retry_count
                print(f"Sleeping for {sleep_time} seconds before retry...")
                time.sleep(sleep_time)

                # Send the event back to the SQS queue with updated retry count
                send_event_to_sqs(event, retry_count)

                return {
                    'statusCode': 500,
                    'body': json.dumps(f'Retrying... (Retry {retry_count})')
                }

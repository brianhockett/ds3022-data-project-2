from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import boto3
import json
import time

@dag(
  'dp2-dag-5',
  start_date = None,
  schedule = None,
  catchup = False,
)

def dp2_dag():

    @task(retries = 10, retry_delay = timedelta(seconds = 10))
    def api_request():
        """
        Sends API request and stores/returns the response in payload variable
        """
        try:
            url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/mgh2xx"

            payload = requests.post(url).json()
            return payload['sqs_url']
        except Exception as e:
            print(f"Error in API call: {e}")
            raise e
    
    @task(retries = 2, retry_delay = timedelta(seconds = 5))
    def message_intake(sqs_url):
        """
        Systematically checks status of SQS queue, receiving messages when they become visible,
        and storing them in a dictionary to be saved as a json file
        """
        # Connect to SQS service
        sqs = boto3.client('sqs', region_name = "us-east-1")
        messages = {}

        # Initial values for consecutive empty message logic
        consec_empty_polls = 0
        max_empty_polls = 10

        # Looping until break is called
        while True:
            try:
                # Getting the queue attributes from the SQS queue returned by the API call
                queue_attributes = sqs.get_queue_attributes(
                    QueueUrl= sqs_url,
                    AttributeNames = [
                        "ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                        "ApproximateNumberOfMessagesDelayed",
                    ],
                )["Attributes"]

                # Monitoring number of visible, invisible, and delayed messages in the queue
                visible = int(queue_attributes["ApproximateNumberOfMessages"])
                invisible = int(queue_attributes["ApproximateNumberOfMessagesNotVisible"])
                delayed = int(queue_attributes["ApproximateNumberOfMessagesDelayed"])
                total = visible + invisible + delayed
                print(f"Queue status: Visible: {visible}, Invisible: {invisible}, Delayed: {delayed}")
            except Exception as e:
                print(f"Error getting queue attributes: {e}")
                continue

            try:
                # Receiving message from SQS queue
                response = sqs.receive_message(
                    QueueUrl = sqs_url,
                    MaxNumberOfMessages = 10,
                    MessageAttributeNames = ["All"],
                    MessageSystemAttributeNames = ["All"],
                    VisibilityTimeout = 60,
                    WaitTimeSeconds = 10,
                )
            except Exception as e:
                print(f"Error receiving messages: {e}")
                continue

            # Checking to see whether message response is an actual message or not
            if "Messages" not in response:

                # Adding to consecutive empty responses value
                consec_empty_polls += 1

                # If there are 0 combined visible, invisible, or delayed messages, break the loop
                if total == 0:
                    print("All messages processed and queue empty.")
                    break

                # If 10 consecutive responses are empty, break the loop
                if consec_empty_polls >= max_empty_polls:
                    print(f"No messages after {max_empty_polls} polls. Assuming complete.")
                    break

                # Sleep for 30 seconds, no need to continuously check for messages
                print("No visible messages right now. Retrying in 30 seconds!")
                time.sleep(30)
                continue
            
            # Reset consecutive empty responses counter (only happens if a message is not empty)
            consec_empty_polls = 0

            # Loop through the received messages
            for message in response["Messages"]:
                try:
                    # Get receipt handle for deletion
                    receipt_handle = message["ReceiptHandle"]

                    # Get order_no and word values, store in the messages dictionary
                    order_no = message["MessageAttributes"]["order_no"]["StringValue"]
                    word = message["MessageAttributes"]["word"]["StringValue"]
                    messages[order_no] = word

                    # Delete the message after it has been put in the dictionary
                    sqs.delete_message(QueueUrl = sqs_url, ReceiptHandle = receipt_handle)
                    print(f"Received and deleted message {order_no}: {word}")

                except Exception as e:
                    print(f"Error deleting or processing message: {e}")


        # Save the message dictionary locally as a json file
        try:
            with open('messages.json', "w") as json_file:
                json.dump(messages, json_file, indent = 4)
            print(f"Dictionary successfully saved to messages.json")
        except Exception as e:
            print(f"Error saving dictionary to file: {e}")

        # Return the dictionary
        return messages
    
    @task(retries = 2, retry_delay = timedelta(seconds = 10))
    def reassemble(messages):
        """
        Reassembles the message in the correct oder according to order_no
        """
        # Converts the keys from strings to ints, sorts the dictionary by the key value (order_no)
        converted_messages = {int(key) : value for key, value in messages.items()}
        sorted_messages = sorted(converted_messages.items())

        # Put the full phrase together in order, then prints and returns the phrase
        full_message = " ".join(word for order_no, word in sorted_messages)
        print(full_message)
        return full_message
    
    @task(retries = 2, retry_delay = timedelta(seconds = 10))
    def submit(uvaid, phrase, platform):
        """
        Sends reassembled message to submission queue
        """

        # Connect to SQS service
        sqs = boto3.client('sqs', region_name = "us-east-1")
        submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

        try:
            # Send submission message, with computing id, the complete phrase, and the platform
            submission_response = sqs.send_message(
                QueueUrl = submission_url,
                MessageBody = phrase, # Ask about this
                MessageAttributes = {
                    'uvaid' : {
                        'DataType' : 'String',
                        'StringValue' : uvaid
                    },
                    'phrase' : {
                        'DataType' : 'String',
                        'StringValue' : phrase
                    },
                    'platform' : {
                        'DataType' : 'String',
                        'StringValue' : platform
                    }
                }
            )
            print(f"Response {submission_response}")
        except Exception as e:
            print(f"Error submitting message: {e}")
            raise e
    
    # Establish the DAG dependencies
    sqs_url = api_request()
    messages = message_intake(sqs_url)
    full_message = reassemble(messages)
    submit('mgh2xx', full_message, 'airflow')

# Run the DAG
dp2_dag()
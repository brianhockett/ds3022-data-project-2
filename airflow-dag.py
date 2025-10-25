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
        Sends API request and stores response in payload variable
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
        Systematically populates SQS queue with received messages
            - Monitor queue for new messages
            - Intakes and stores messages
            - Deletes messages after intake
        """
        sqs = boto3.client('sqs', region_name = "us-east-1")
        messages = {}
        print("Testing Logs")

        consec_empty_polls = 0
        max_empty_polls = 10
        while True:
            try:
                queue_attributes = sqs.get_queue_attributes(
                    QueueUrl= sqs_url,
                    AttributeNames = [
                        "ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                        "ApproximateNumberOfMessagesDelayed",
                    ],
                )["Attributes"]

                visible = int(queue_attributes["ApproximateNumberOfMessages"])
                invisible = int(queue_attributes["ApproximateNumberOfMessagesNotVisible"])
                delayed = int(queue_attributes["ApproximateNumberOfMessagesDelayed"])
                total = visible + invisible + delayed
                print(f"Queue status: Visible: {visible}, Invisible: {invisible}, Delayed: {delayed}")
            except Exception as e:
                print(f"Error getting queue attributes: {e}")
                continue

            try:
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

            if "Messages" not in response:
                consec_empty_polls += 1
                if total == 0:
                    print("All messages processed and queue empty.")
                    break
                if consec_empty_polls >= max_empty_polls:
                    print(f"No messages after {max_empty_polls} polls. Assuming complete.")
                    break


                print("No visible messages right now. Retrying in 30 seconds!")
                time.sleep(30)
                continue
                
            consec_empty_polls = 0

            for message in response["Messages"]:
                try:
                    receipt_handle = message["ReceiptHandle"]
                    order_no = message["MessageAttributes"]["order_no"]["StringValue"]
                    word = message["MessageAttributes"]["word"]["StringValue"]
                    messages[order_no] = word

                    sqs.delete_message(QueueUrl = sqs_url, ReceiptHandle = receipt_handle)
                    print(f"Received and deleted message {order_no}: {word}")

                except Exception as e:
                    print(f"Error deleting or processing message: {e}")


            
        try:
            with open('messages.json', "w") as json_file:
                json.dump(messages, json_file, indent = 4)
            print(f"Dictionary successfully saved to messages.json")
        except Exception as e:
            print(f"Error saving dictionary to file: {e}")

        return messages
    
    @task(retries = 2, retry_delay = timedelta(seconds = 10))
    def reassemble(messages):
        """
        Reassembles the message in the correct oder according to order_no
        """
        converted_messages = {int(key) : value for key, value in messages.items()}
        sorted_messages = sorted(converted_messages.items())
        full_message = " ".join(word for order_no, word in sorted_messages)
        print(full_message)
        return full_message
    
    @task(retries = 2, retry_delay = timedelta(seconds = 10))
    def submit(uvaid, phrase, platform):
        """
        Sends reassembled message to submission queue
        """
        sqs = boto3.client('sqs', region_name = "us-east-1")
        submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

        try:
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
        
    sqs_url = api_request()
    messages = message_intake(sqs_url)
    full_message = reassemble(messages)
    submit('mgh2xx', full_message, 'airflow')

dp2_dag()
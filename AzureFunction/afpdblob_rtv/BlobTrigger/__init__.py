import logging
import os, json
from azure.storage.queue import QueueService, QueueMessageFormat
import azure.functions as func
import base64
from datetime import datetime


def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name[len(os.environ['remoteStorageInputContainer'])+1:]}\n"
                 f"Blob Size: {myblob.length} bytes")

    name = myblob.name[len(os.environ['remoteStorageInputContainer'])+1:]
    logging.info(name)

    queue_service = QueueService(account_name=os.environ['remoteStorageAccountName'], account_key=os.environ['remoteStorageAccountKey'])
    queue_service.encode_function = QueueMessageFormat.text_base64encode

    now = datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S%Z")
    video = "{\"filename\":\"" + name + "\", \"location\":\"Utrecht\", \"track\":\"5b\", \"timestamp\":\"" + now + "\"}"
    #video_base64 = base64.b64encode(bytes(video, 'utf-8'))

    queue_service.put_message(os.environ['AzureQueueName'], video)
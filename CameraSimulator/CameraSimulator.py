# Make sure you installed azure.storage.blog using pip install azure-storage-blob


from azure.storage.blob import (
    BlockBlobService,
)
from datetime import datetime

outputConnectionString = "DefaultEndpointsProtocol=http;BlobEndpoint=http://<<public ip of your Ubuntu VM>>:11002/localvideostor;AccountName=localvideostor;AccountKey=xpCr7otbKOOPw4KBLxtQXdG5P7gpDrNHGcrdC/w4ByjMfN4WJvvIU2xICgY7Tm/rsZhms4Uy4FWOMTeCYyGmIA==;"
outputContainer = 'localvideoblob'
now = datetime.strftime(datetime.now(), "%Y%m%dT%H%M%S%Z")
inputFileName = now + "video.MP4" # "text1.mp4" # HDV_0712.MP4

outputBlob = BlockBlobService(connection_string=outputConnectionString)
print("create container")
outputBlob.create_container(outputContainer, fail_on_exist=False)
print("end creating container")

print("start writing to local blob")
outputBlob.create_blob_from_path(outputContainer, inputFileName, "<location of video on local disk>")
print("end writing to local blob")

#
# Use the following code if you want to send a message with metadata to IoT Hub
#

#from iothub_client import IoTHubClient, IoTHubTransportProvider, IoTHubMessage
import time
import os

#CONNECTION_STRING = "<your IoT Hub connection string>"
#PROTOCOL = IoTHubTransportProvider.MQTT

#client = IoTHubClient(CONNECTION_STRING, PROTOCOL)
#message = IoTHubMessage(
#    "{\"filename\":\"" + inputFileName + "\", \"location\":\"Utrecht\", \"track\":\"5b\", \"timestamp\":\"" + now + "\"}")
#client.send_event_async(message, send_confirmation_callback, None)
#time.sleep(5)
#print("Message transmitted to IoT Hub")

#exit(0)

#def send_confirmation_callback(message, result, user_context):
#    print("Confirmation received for message with result = %s" % (result))


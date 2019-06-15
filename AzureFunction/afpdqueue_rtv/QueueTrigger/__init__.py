import logging
import os, json
from azure.storage.blob import (
    AppendBlobService,
    BlockBlobService,
    BlobPermissions,
    ContainerPermissions
)
import azure.functions as func
from io import BytesIO
from datetime import datetime, timedelta
from PIL import Image
import numpy as np
import sys
import cv2
import requests
import math
import urllib.request as urllib2
import ssl
import time

def main(msg: func.QueueMessage) -> None:
    # consume videoname from event that is put on blob storage using filename

    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))

    raw = msg.get_body().decode('utf-8')
    logging.info(raw)
    eventVideo=json.loads(raw)

    blockBlobService = BlockBlobService(account_name=os.environ['remoteStorageAccountName'], account_key=os.environ['remoteStorageAccountKey'])
    appendBlobService = AppendBlobService(account_name=os.environ['remoteStorageAccountName'], account_key=os.environ['remoteStorageAccountKey'])

    cap = getVideo(blockBlobService, eventVideo)
    folder = eventVideo["filename"]
    
    # process video
    x=1
    frameRate = cap.get(5) #frame rate
    numberOfPicturesPerSecond= int(os.environ["numberOfPicturesPerSecond"])
    # start creating frames from video
    while(cap.isOpened()):
        frameId = cap.get(1) #current frame number
        ret, frame = cap.read()
        if (ret != True):
            break

        # in case frame matches a multiple of the frame, create image
        if frameId  % math.floor(frameRate/numberOfPicturesPerSecond) == 0:
            logging.info("create cap" + str(x))
            # convert frame to PIL image
            frame_conv = cv2.cvtColor(frame,cv2.COLOR_BGR2RGB)
            pilImage = Image.fromarray(frame_conv)
            imgByteArr = BytesIO()
            pilImage.save(imgByteArr, format='PNG')
            imgByteArr = imgByteArr.getvalue()

            # write image to blob for logging
            imageFileName= folder + "/log/image" +  str(int(x)) + "_img.png"
            blockBlobService.create_blob_from_bytes(os.environ['remoteStorageOutputContainer'], imageFileName, imgByteArr)

            # try to recognize objects and text from image
            description, caption, confidence, text, fullText = analyzeImage(imgByteArr)
            # in case trainnumber is recognized of train, public to Power BI
            trainNumber=""
            if (len(text) == 4 and text.isdigit()) or text[:2] == "NL":
                trainNumber = text
                publishPowerBI(blockBlobService, x, imgByteArr, folder, eventVideo, text, caption)

            # write identification of image to csv
            loggingCsv = "\n" + str(imageFileName) + "|" + str(caption) + "|" + str(confidence) + "|" + str(json.dumps(text).replace('|', ':pipe')) + "|" + str(trainNumber) + "|" + str(json.dumps(fullText).replace('|', ':pipe'))
            appendBlobService.append_blob_from_text("logging", os.environ["loggingcsv"], loggingCsv)
                
            # increment image
            x+=1

def getVideo(blockBlobService: BlockBlobService, eventVideo: list):

    # Check if file is already in blob storage, since synchronizing video from blob on edge to blob Azure can take some time
    retry=1
    while True:
        logging.info("File exists on blob, attempt: " + str(retry))
        if blockBlobService.exists(os.environ['remoteStorageInputContainer'], eventVideo["filename"]):
            break
        elif retry > 60:
            logging.info("Timeout, video " + eventVideo["filename"] + " cannot be read from blob")  
            # todo put on dead letter queue such that it can be processed later
            raise Exception("Timeout, video " + eventVideo["filename"] + " cannot be read from blob")          
        else:
            time.sleep(5)
            retry+=1

    # create SAS url such that video can be read from blob storage using opencv directly
    sasTokenRead = blockBlobService.generate_container_shared_access_signature(
        os.environ['remoteStorageInputContainer'],
        ContainerPermissions.READ,
        datetime.utcnow() + timedelta(hours=1),
    )
    readURL = os.environ['storUrl'] + os.environ['remoteStorageInputContainer'] + "/" + eventVideo["filename"] + "?" + sasTokenRead
    logging.info(readURL)

    # read video using opencv
    retry=1
    while True:
        # loop, since synchronizing video from blob on edge to blob Azure can take some time
        logging.info("Video opening attempt: " + str(retry))
        cap = cv2.VideoCapture(readURL)
        if cap.isOpened():
            logging.info("video opened: " + str(cap.isOpened()))
            break
        elif retry > 5:
            logging.info("Timeout, video " + eventVideo["filename"] + " cannot be openend")  
            raise Exception("Timeout, video " + eventVideo["filename"] + " cannot be openend")          
        else:
            time.sleep(5)
            retry+=1

    return cap

def analyzeImage(imgByteArr: Image):

    # set headers
    cognitiveHeaders    = {'Ocp-Apim-Subscription-Key': os.environ['cognitiveServiceKey'],
                           'Content-Type': 'application/octet-stream'}
    cognitiveParams     = {'visualFeatures': 'Categories,Description,Color'}

    # Use cogntive service with computer service to identify objects in train
    response = requests.request('post', os.environ["visionUrl"], json=json, data=imgByteArr, headers=cognitiveHeaders, params=cognitiveParams)
    response.raise_for_status()
    description = response.json()['description']['tags']
    caption=""
    confidence=""
    if response.json()['description']['captions']:
        i=0
        caption = ""
        confidence = ""
        while i < len(response.json()['description']['captions']):
            if caption == "":
                caption = response.json()['description']['captions'][i]['text']
            else:
                caption += ", " + response.json()['description']['captions'][i]['text']          
            
            if confidence == "":           
                confidence=response.json()['description']['captions'][i]['confidence']
            else:
                confidence += ", " + response.json()['description']['captions'][i]['confidence']
            i+=1            

        logging.info("caption: " + json.dumps(caption)) 
        logging.info("confidence: " + json.dumps(confidence)) 

    # Use cogntive service with ocr service to recognize text in train
    response = requests.request('post', os.environ["ocrUrl"], json=json, data=imgByteArr, headers=cognitiveHeaders, params=cognitiveParams)
    response.raise_for_status()

    text=""
    fullText=response.json()['regions']
    if fullText:
        i=0
        j=0
        k=0
        while i < len(response.json()['regions']):
            while j < len(response.json()['regions'][i]['lines']):
                while k < len(response.json()['regions'][i]['lines'][j]['words']):
                    if text == "":
                        text=response.json()['regions'][i]['lines'][j]['words'][k]['text']
                    else:
                        text += " " + response.json()['regions'][i]['lines'][j]['words'][k]['text']
                    k+=1
                j+=1
            i+=1
        logging.info(json.dumps(text))

    return description, caption, confidence, text, fullText

def publishPowerBI(blockBlobService: BlockBlobService, x:int, imgByteArr: Image, folder: str, eventVideo:list, text: str, caption: str):

    blobOutputTrainNumber = 'trainnumber/' + eventVideo["filename"] + "_" + text + "_" + str(x) + ".png"
    blockBlobService.create_blob_from_text(os.environ['remoteStorageOutputContainer'], blobOutputTrainNumber, imgByteArr)
    # create SAS url such that video can be read from blob storage
    sasTokenRead = blockBlobService.generate_blob_shared_access_signature(
                        os.environ['remoteStorageOutputContainer'],
                        blobOutputTrainNumber,
                        BlobPermissions.READ,
                        datetime.utcnow() + timedelta(hours=1)
    )
    sasPictureTrainNumber = os.environ['storUrl'] + os.environ['remoteStorageOutputContainer'] + "/" + blobOutputTrainNumber + "?" + sasTokenRead

    #write data to Power BI as streaming data set
    streamingDataStr = '[{{"location": "{0}", "track": "{1}", "time": "{2}", "trainNumber": "{3}", "probGraffiti": "{4}", "caption": "{5}", "sasPictureTrainNumber":"{6}", "sasPictureGraffiti":"{7}"}}]'.format(eventVideo["location"], eventVideo["track"], eventVideo["timestamp"], text, 0.0, caption, sasPictureTrainNumber,"").encode("utf-8")
                    
    try:
        req = urllib2.Request(os.environ['powerBIConnectionString'], streamingDataStr)
        gcontext = ssl.SSLContext()
        response = urllib2.urlopen(req, context=gcontext)
        logging.info("POST request to Power BI with data:{0}".format(streamingDataStr))
        logging.info("Response: HTTP {0} {1}\n".format(response.getcode(), response.read()))
    except:
        logging.info("Writing to Power BI dashboard failed, no breaking error, continue")
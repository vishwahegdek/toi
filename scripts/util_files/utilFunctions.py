import os
import hashlib
import datetime
from kafka import KafkaProducer
import json

from class_files.postgres_class import *

from logger_config import get_logger
logger = get_logger('util')

#Function to get checksum of file
def getFileChecksum(filePath):
    sha256Hash = hashlib.sha256()
    with open(filePath, "rb") as file:
        for byteBlock in iter(lambda: file.read(4096), b""):
            sha256Hash.update(byteBlock)
    return sha256Hash.hexdigest()


def moveFile(sourcefilePath, destinationFilePath):
    if os.path.exists(sourcefilePath):
        os.rename(sourcefilePath, destinationFilePath)
        return True
    else:
        print(f"Error: File not found at {sourcefilePath}")
        logger.error(f"Error: File not found at {sourcefilePath}")
        return False


def handle_error(sourcefilePath, errorMessage, errorDirPath, sysFileName, ingestionSourceId, ingestionType, ingestionFormat):
    print(f"Error processing file: {sourcefilePath}")
    logger.error(f"Error processing file: {sourcefilePath}")
    destinationFilePath = os.path.join(os.path.dirname(errorDirPath), sysFileName)
    print(f"Error file renamed to: {destinationFilePath}")
    logger.error(f"Error file renamed to: {destinationFilePath}")

    if(os.path.exists(sourcefilePath)):
        moveStat = moveFile(sourcefilePath, destinationFilePath)
        if(moveStat):
            print("Moved to Error Folder")
            logger.info("Moved to Error Folder")
        else:
            print("Unable to move due to an error!!")
            logger.error("Unable to move due to an error!!")
            return
    else:
        print("Source file not present in the given path")
        logger.info("Source file not present in the given path")
        return
    
    dataToInsert = {
            "original_filename": sourcefilePath.split("/")[-1],
            "system_filename": sysFileName,
            "ingestion_source": ingestionSourceId,
            "ingestion_type": ingestionType,
            "ingestion_format": ingestionFormat,
            "checksum": getFileChecksum(destinationFilePath),
            "csv_file_format": None,
            "status": "Failure", 
            "failure_reason": errorMessage,
            "created_on": int(datetime.datetime.now().timestamp())
        }

    try:
        dbConnect = postgresDb()
        dbConnect.insertIntoDb(dataToInsert=dataToInsert)
        print("Data Logged to Database Successfully")
        logger.info("Data Logged to Database Successfully")
    except:
        print("Unable to Log to Database!!")
        logger.error("Unable to Log to Database!!")
        return

#Function to Push Data to KAFKA Topic, Data must be a JSON(dict) of Key,Value Pairs
def pushDataToKafkaTopic(topicName, data, bootstrap_server, headers):
    #Create Kafka Producer Instance
    producer = KafkaProducer(bootstrap_servers = bootstrap_server)

    for dataKey in data:
        # print(topicName, key, json.dumps(data[key]))
        # producer.send(topicName, json.dumps({"hashKey":key}), json.dumps(data[key]))
        producer.send(topicName, key=dataKey.encode('utf-8'), value=json.dumps(data[dataKey]).encode('utf-8'), headers=headers)

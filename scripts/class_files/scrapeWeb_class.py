import configparser
import sched
import time
import datetime
import sys

from class_files.postgres_class import *
from util_files.toiHeadlinesScraper import *
from util_files.utilFunctions import *

from logger_config import get_logger

logger = get_logger('scrape')

class scrapeFromWeb():

    def __init__(self):
        config = configparser.ConfigParser(allow_no_value=True)
        config.read("config.ini")
        self.inputType = config["web_scraper"]["input_type"]
        self.url=config["web_scraper"]["url"]
        self.kafkaTopicName = config["web_scraper"]["kafka_topic_name"]
        self.kafkaBootstrapServer = config["web_scraper"]["kafka_bootstrap_server"]
        self.intervalTime = config["web_scraper"]["scraping_interval"] #in mins
        self.scheduler = sched.scheduler(time.time, time.sleep)
        self.processedFolderPath = config["web_scraper"]["processed_folder_path"]
        self.errorFolderPath = config["web_scraper"]["error_folder_path"]

         #Params to add to Database
        self.sourceId=config["web_scraper"]["source_connector_id"]
        self.ingestionType="Stream"
        self.ingestionFormat="HTML"

        self.dbConnect = postgresDb()

    def runScraper(self):
        currentTimeStamp = str(int(datetime.datetime.now().timestamp()))
        systemFilename = currentTimeStamp +"__TOIWebScrapingFromUrl.html"
        
        pageResponse = None
        try:
            newsData, pageResponse = timesOfIndia(self.inputType, self.url)
        except:
            print("Error Scraping data from web")
            logger.error("Error Scraping data from web")
            return
        
        if(newsData != {}):
            print("Data scraped from web Successfully")
            logger.info("Data scraped from web Successfully")
            try:
                kafkaHeaders = [
                    ("FileName", systemFilename.encode('utf-8')),
                    ("IngestionTimestamp", currentTimeStamp.encode('utf-8'))
                ]
                pushDataToKafkaTopic(self.kafkaTopicName, newsData, self.kafkaBootstrapServer, kafkaHeaders)
                print("Data pushed to Kafka Topic successfully")
                logger.info("Data pushed to Kafka Topic successfully")
            except:
                print("Unable to push data Kafka Topic")
                logger.error("Unable to push data Kafka Topic")
                destinationFilePath = os.path.join(os.path.dirname(self.processedFolderPath), systemFilename)
                dumpHtmlToFile(pageResponse, destinationFilePath)
                print("Html data dumped to file at error location due to failure")
                logger.error("Html data dumped to file at error location due to failure")
                dataToInsert = {
                    "original_filename": None,
                    "system_filename": systemFilename,
                    "ingestion_source": self.sourceId,
                    "ingestion_type": self.ingestionType,
                    "ingestion_format": self.ingestionFormat,
                    "checksum": getFileChecksum(destinationFilePath),
                    "csv_file_format": None,
                    "status": "Failure", 
                    "failure_reason": "Unable to push data Kafka Topic",
                    "created_on": int(datetime.datetime.now().timestamp())
                }
                try:
                    self.dbConnect.insertIntoDb(dataToInsert=dataToInsert)
                    print("Data Logged to Database Successfully")
                    logger.info("Data Logged to Database Successfully")
                except:
                    print("Unable to Log to Database!!")
                    logger.error("Unable to Log to Database!!")
                    return
        else:
            print("Unable to scrape Data!! Wrong url provided!! Pls check the url and try Again!!")
            logger.error("Unable to scrape Data, Wrong url provided")
            return
        
        destinationFilePath = os.path.join(os.path.dirname(self.processedFolderPath), systemFilename)
        dumpHtmlToFile(pageResponse, destinationFilePath)
        print("Html data dumped to file at processed location successfully")
        logger.info("Html data dumped to file at processed location successfully")
        dataToInsert = {
            "original_filename": None,
            "system_filename": systemFilename,
            "ingestion_source": self.sourceId,
            "ingestion_type": self.ingestionType,
            "ingestion_format": self.ingestionFormat,
            "checksum": getFileChecksum(destinationFilePath),
            "csv_file_format": None,
            "status": "Success", 
            "failure_reason": None,
            "created_on": int(datetime.datetime.now().timestamp())
        }

        try:
            self.dbConnect.insertIntoDb(dataToInsert=dataToInsert)
            print("Data Logged to Database Successfully")
            logger.info("Data Logged to Database Successfully")
        except:
            print("Unable to Log to Database!!")
            logger.error("Unable to Log to Database!!")
            return
        
        return


    def schedule_next_event(self):
        self.scheduler.enter(int(float(self.intervalTime)*60), 1, self.schedule_next_event)
        print(f"Scheduled Next Web Scraping Process to run after {self.intervalTime} Minutes")
        self.runScraper()

    def start(self):
        # self.scheduler.enter(0, 1, self.schedule_next_event)
        print("Scheduled First Web Scraping Process")
        self.runScraper()  # Run the scraper immediately once
        self.schedule_next_event()
        self.scheduler.run()
        
    def stop(self):
        for event in self.scheduler.queue:
            self.scheduler.cancel(event)
        print("Stopped Web Scraping Process")
        sys.exit(0)
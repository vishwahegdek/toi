import psycopg2
import configparser

class postgresDb:
    
    def __init__(self):
        config = configparser.ConfigParser(allow_no_value=True)
        config.read("config.ini")
        
        self.dbName = config.get("database", "name")
        self.dbUser = config.get("database", "user")
        self.dbPassword = config.get("database", "password")
        self.dbHost = config.get("database", "host")
        self.dbPort = config.get("database", "port")
        self.tableName = config.get("database", "table_name")

        self.insertValuesHeaders = ["original_filename", #Original File Name incase it is batch/file ingestion
                                    "system_filename",#System Generated File Name
                                    "ingestion_source",#What is the source - id or name of the source connector
                                    "ingestion_type", #Batch or Stream - Bacth refers to file ingestion and Stream refers to direct from web or socket
                                    "ingestion_format",# Which data format - PCAP, CDR, WebScraping etc.....,
                                    "checksum", # Checksum of the file
                                    "csv_file_format", # If it is a csv, then what is the format of file identified
                                    "status", # Success, Failure
                                    "failure_reason", #Failure Reason
                                    "created_on" #CreatedOn Timestamp
                                    ]

    def connectToDb(self):
        self.connection = psycopg2.connect(dbname=self.dbName, 
                                      user=self.dbUser, 
                                      password=self.dbPassword, 
                                      host=self.dbHost, 
                                      port=self.dbPort)
        
        return
    
    def closeDbConnection(self):
        
        self.connection.close()
        
        return

    '''
    The dataToInsert must be a json consisting of following fields
    {
        original_filename, - Original File Name incase it is batch/file ingestion
        system_filename, - System Generated File Name
        ingestion_source, - What is the source - id or name of the source connector
        ingestion_type, - Batch or Stream - Bacth refers to file ingestion and Stream refers to direct from web or socket
        ingestion_format, - Which data format - PCAP, CDR, WebScraping etc.....,
        checksum, - Checksum of the file
        csv_file_format, - If it is a csv, then what is the format of file identified
        status, - Success, Failure
        failure_reason,
        created_on
    }
    '''
    def insertIntoDb(self, dataToInsert):

        insertValuesList = []
        for index in range(0, len(self.insertValuesHeaders)):
            insertValuesList.append(dataToInsert[self.insertValuesHeaders[index]])

        print(tuple(insertValuesList))

        try:
            self.connectToDb()
            cursor = self.connection.cursor()

            insertQuery = """
                INSERT INTO """+str(self.tableName)+ """ (
                    original_filename,
                    system_filename,
                    ingestion_source,
                    ingestion_type,
                    ingestion_format,
                    checksum,
                    csv_file_format,
                    status,
                    failure_reason,
                    created_on
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))
            """

            print(insertQuery)
            cursor.execute(insertQuery, tuple(insertValuesList))
            self.connection.commit()
            cursor.close()
        
        except psycopg2.DataError as e:
            print(f"Ignoring database insertion error: {str(e)}")

        except Exception as e:
            print(f"Error inserting into database: {str(e)}")

        finally:
            self.closeDbConnection()
from class_files.scrapeWeb_class import *

webScraper = scrapeFromWeb()
try:
    webScraper.start()
except KeyboardInterrupt:
    webScraper.stop()
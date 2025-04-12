import requests
from bs4 import BeautifulSoup
import json

from logger_config import get_logger
logger = get_logger('scraper')

#Function to generate hash key for a given text
def getHashValue(textdata):
    hashVal = hash(textdata)
    if(hashVal < 0):
        hashVal = hashVal*-1
    
    return hashVal

#Function to generate JSON for each newsline
def createNewsJson(categories, newsLine, url, content, images):
    data = {
        "categories": categories,
        "news": newsLine,
        "url": url,
        "content": content,
        "images": images  # List of image URLs
    }
    return data

def scrapeArticleContent(url):
    try:
        response = requests.get(url)
        articleSoup = BeautifulSoup(response.content, "html.parser")
        
        # Find content div and image div based on ToI website's structure
        contentDiv = articleSoup.find('div', {'class': '_s30J'})
        imgDiv = articleSoup.find('div', {'class': 'T22zO'})
        
        # Initialize variables to store content and images
        content = ""
        images = []
        
        # Extract text content
        if contentDiv:
            content = contentDiv.get_text(separator='\n', strip=True)
        else:
            content = "Content not available"
        
        # Extract image URLs
        if imgDiv:
            img_tags = imgDiv.find_all('img')
            for img_tag in img_tags:
                img_src = img_tag.get('src')
                if img_src:
                    images.append(img_src)
        
        return content, images
    
    except Exception as e:
        print(f"Failed to scrape content for URL {url}: {e}")
        logger.error(f"Failed to scrape content for URL {url}: {e}")
        return "Failed to retrieve content"

def scrapeToiHeadlines(soup):
    toiNews = {}
    base_url = "https://timesofindia.indiatimes.com"
    #News Headlines Scraping
    #Scraping the Top Div
    for mainDiv in soup.find_all('div', {'id': 'c_0201'}):
        for innerDiv in mainDiv:
            #Scrape News Headlines Section
            if(innerDiv['id'] == "c_headlines_wdt_1"):
                mainCategory = innerDiv.h1.text
                #Scraping Top Pane of News Headlines
                categories = mainCategory+","+"Top Pane Of News Headlines"
                print(categories)
                for innerDivItem in innerDiv.find_all('div', {'class':'top-newslist'}):
                    for listItem in innerDivItem.find_all('li'):
                        headline = listItem.text.strip()
                        link = listItem.find('a')['href']
                        full_url = base_url + link
                        content, images = scrapeArticleContent(full_url)
                        headlineDict = createNewsJson(categories, headline, full_url, content, images)
                        hashKey = str(getHashValue(headlineDict["news"]))
                        toiNews[hashKey] = headlineDict
                #Scraping Bottom Pane of News Headlines
                categories = mainCategory+","+"Bottom Pane Of News Headlines"
                for innerDivItem in innerDiv.find_all('div', {'class':'headlines-list'}):
                    for listitem in innerDivItem.find_all('li'):
                        headline = listitem.text.strip()
                        link = listitem.find('a')['href']
                        full_url = base_url + link
                        content, images = scrapeArticleContent(full_url)
                        headlineDict = createNewsJson(categories, headline, full_url, content, images)
                        hashKey = str(getHashValue(headlineDict["news"]))
                        toiNews[hashKey] = headlineDict
            #Scrape MetroCities Section
            if(innerDiv['id'] == "c_020101"):
                mainCategory=innerDiv.h2.text
                mcDiv = innerDiv.find_all('div', {"id":"c_headlines_wdt_1"})[0]
                for innerDivItem in mcDiv.find_all(['h2', 'div'], {"class":["heading2","top-newslist", "headlines-list"]}):
                    #Scrape the Cities Name
                    if("heading2" in ' '.join(innerDivItem['class'])):
                        categories = mainCategory+","+innerDivItem.a.text
                    #Scrape all the news for the given cities
                    elif("top-newslist" in ' '.join(innerDivItem['class']) or "headlines-list" in ' '.join(innerDivItem['class'])):
                        for listitem in innerDivItem.find_all('li'):
                            headline = listitem.text.strip()
                            link = listitem.find('a')['href']
                            full_url = base_url + link
                            content, images = scrapeArticleContent(full_url)
                            headlineDict = createNewsJson(categories, headline, full_url, content, images)
                            hashKey = str(getHashValue(headlineDict["news"]))
                            toiNews[hashKey] = headlineDict
                #Scraping Business News Section
                bnDiv = innerDiv.find_all('div', {"id":"c_headlines_wdt_2", "class":"business"})[0]
                for innerDivItem in bnDiv:
                    #Scraping News Heading
                    if("heading1" in " ".join(innerDivItem['class'])):
                        mainCategory = innerDivItem.a.text
                    #Scraping SubHeading
                    if("business" in " ".join(innerDivItem['class'])):
                        categories = mainCategory + "," + innerDivItem.h4.text
                    #Scraping News Lines
                        for listitem in innerDivItem.find_all('li'):
                            headline = listitem.text.strip()
                            link = listitem.find('a')['href']
                            full_url = base_url + link
                            content, images = scrapeArticleContent(full_url)
                            headlineDict = createNewsJson(categories, headline, full_url, content, images)
                            hashKey = str(getHashValue(headlineDict["news"]))
                            toiNews[hashKey] = headlineDict
                #Scraping World News Section
                wndiv = innerDiv.find_all('div', {"id":"c_headlines_wdt_3"})[0]
                for innerDivItem in wndiv:
                    #Scraping News Heading
                    mainCategory = innerDivItem.find("h2").text
                    for item in wndiv.find_all(['h4', 'ul'], {"class":["heading2", "news_card"]}):
                        #Scraping Subheadings
                        if("heading2" in ' '.join(item['class'])):
                            categories = mainCategory+","+item.text
                        else:
                            #Scraping News Lines
                            for listitem in item.find_all('li'):
                                headline = listitem.text.strip()
                                link = listitem.find('a')['href']
                                full_url = base_url + link
                                content, images = scrapeArticleContent(full_url)
                                headlineDict = createNewsJson(categories, headline, full_url, content, images)
                                hashKey = str(getHashValue(headlineDict["news"]))
                                toiNews[hashKey] = headlineDict
                #Scraping Bottom 3 columned news
                col3ndiv = innerDiv.find_all('div', {"id":"c_0201010104"})[0]
                #Scraping News Type
                for innerDivItems in col3ndiv:
                    mainCategory = innerDivItems.h2.text
                    for item in innerDivItems.find_all(['h4','ul']):
                        # Scraping Subheading(Sports Type)
                        if("heading2" in " ".join(item['class'])):
                            categories = mainCategory+","+item.text
                        else:
                            for listitem in item.find_all('li'):
                                headline = listitem.text.strip()
                                link = listitem.find('a')['href']
                                full_url = base_url + link
                                content, images = scrapeArticleContent(full_url)
                                headlineDict = createNewsJson(categories, headline, full_url, content, images)
                                hashKey = str(getHashValue(headlineDict["news"]))
                                toiNews[hashKey] = headlineDict
    with open('news.json', 'w') as f:
        json.dump(toiNews,f)
    return toiNews

def timesOfIndia(inputType, scrapingPath):
    if(inputType == "URL"):
        url = scrapingPath
        pageResponse = requests.get(url)
        data = pageResponse.content
        soup = BeautifulSoup(data,"html.parser")
    else:
        filePath = scrapingPath
        with open(filePath, "r", encoding="utf-8") as fp:
            soup = BeautifulSoup(fp.read(), "html.parser")
        toiHeadlines = scrapeToiHeadlines(soup)
        return toiHeadlines
    
    toiHeadlines = scrapeToiHeadlines(soup)
    return toiHeadlines, pageResponse

def dumpHtmlToFile(htmlreponse, destinationPath):

    try:
        with open(destinationPath, 'wb') as file:
            file.write(htmlreponse.content)
        print(f"HTML page downloaded and saved to {destinationPath}")
        logger.info(f"HTML page downloaded and saved to {destinationPath}")
    except:
        print(f"Failed to dump html to given destination path")
        logger.error("Failed to dump html to given destination path")
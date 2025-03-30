import csv
import os
import time
import json
import re
import sys
import logging
import cloudscraper
from dotenv import load_dotenv
from fake_useragent import UserAgent
from datetime import datetime
from bs4 import BeautifulSoup
from requests.exceptions import SSLError, RequestException


startprocess = time.process_time()
start = time.time()

# create a logger for monitoring both in the terminal and for reference in a "logs" folder in a subfolder named after the current date
logs_path = os.path.join("logs", "scraper")
os.makedirs(logs_path, exist_ok=True)
logger = logging.getLogger(__name__)
logging.basicConfig(
    handlers= [
        logging.FileHandler(os.path.join(logs_path, (str(datetime.now().strftime("%Y.%m.%d %H-%M-%S")) + ".log"))),
        logging.StreamHandler()
    ],
    level=logging.INFO,
    format="%(asctime)s: %(message)s",
    datefmt="%Y.%m.%d %H:%M:%S"
    )

## creates global variables for the Scraper class to track the amount of http requests sent and products scraped, which will be documented in the logs file
calls = 0
total_items = 0

## creates a "data" folder and a subfolder named after the current date into which the scraped data will be saved
logger.info("creating directory for data")
data_path = os.path.join("data", datetime.now().strftime('%Y%m%d'))
os.makedirs(data_path, exist_ok=True)

## loads all websites in the websites.json file as a global variable for reference to the Scraper class
with open ("websites.json", "r") as file:
    websites = json.load(file).get("websites", [])

## creates headers, cookies, and a session for the HTTP requests as global variables (to avoid dedundant duplicate variables) for reference to the Scraper class
ua = UserAgent()
headers = {
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://shop.rewe.de/",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0"
    }
with open("cookies.json", "r") as file:
    location = json.load(file).get("locations", {}).get("Tussmannstr. 41-63, 40477 Düsseldorf / Pempelfort")    
load_dotenv()
cookies = {
    "_rdfa": location,
    "cf_clearance": os.getenv("cf_clearance", "")
    }
session = cloudscraper.CloudScraper()




class Scraper:
    """
    scrapes the website it gets assigned.
    tracks the amount of HTTP requests it makes and the amount of products found.
    
    Args:
    string: the URL which it will scrape.

    Output: 
    a csv file of all products found on every page of the URL.
    """
    
    def __init__(self, url):
        self.url = url
        self.page = 1 # current page, gets updated at the end of every successfully scraped page
        self.last_page = 1 # during the scrape method, this will get updated to the actual last page if there is more than one page
        self.products_per_page = 250 # the maximum amount of objects that can be shown on a single page is 250
        self.products = [
            ["ID", "name", "amount", "price in €","€ per kg", "reduced price", "bio label", "category"]
        ] # the header row for the csv file, structured as a list of lists for constructing a csv file 
        self.categoy = re.sub(r"^https?://shop.rewe.de/c/", "", self.url)

    def check_pagination(self, soup):
        """
        checks for pagination in the BeautifulSoup object and updates the self.last_page variable accordingly
        Args:
        BeautifulSoup Class
        """
        lastpage = soup.find_all("button", class_="PostRequestGetFormButton paginationPage paginationPageLink")
        if lastpage:
            self.last_page = int(lastpage[-1].get_text(strip=True))


    def scrape(self):
        """
        scrapes the products of every website listed in the websites.json file

        returns:
        a list of csv files for every website listing the following for every product:
        product ID, name, amount, price, price per kg, whether it has a reduced price, whether it has a bio-label 
        """
        
        logger.info("""
starting to scrape %s""", self.url)
        
        while self.page <= self.last_page: 
            url_page = f"{self.url}/?objectsPerPage={self.products_per_page}&page={self.page}" 
            
            try:
                response = session.get(url_page, cookies=cookies, headers=headers)
                logger.debug(f"status code: {response.status_code}")
                global calls
                calls += 1
                logger.info("successfully reached page %s", self.page)
                soup = BeautifulSoup(response.text, "lxml")
                
                if self.page == 1:
                    self.check_pagination(soup)

                matches = soup.find_all("article")
                for item in matches:
                    global total_items
                    total_items += 1
                    
                    ## gets the unique product ID to be used as the primary key in the database entry for the product
                    product_id = item.find("meso-data")
                    product_id = int(product_id.get("data-productid")) if product_id else ""

                    ## gets the name of the product and cleans the data point using regular expressions
                    name = item.find("div", class_="LinesEllipsis")
                    name = name.text if name else ""
                    name = re.sub(r"""[\"']""", "", name).strip()
                    
                    ## gets the price of the product and turns it into an integer
                    ## checks if the product has a reduced price and assigns either True or False to the "reduced price" data point of the product
                    price = item.find("div", class_="search-service-productPrice productPrice") 
                    if price:
                        offer = False
                        price = price.text
                        price = float(price.replace("€", "").replace(",",".").strip())
                    else:
                        offer = True
                        price = item.find("div", class_="search-service-productOfferPrice productOfferPrice")
                        price = price.text
                        price = float(price.replace("€", "").replace(",",".").strip())
                    
                    ## gets the listed amount of the product 
                    ## gets cleaned up through regular expressions later on after it was used as reference for the price_per_amount data point
                    amount = item.find("div", class_="productGrammage search-service-productGrammage")
                    amount = amount.text if amount else ""
                    
                    ## filters for any mentions of price per kg in the "amount" variable and either extracts it from the variable using regular expressions 
                    ## or calculates it based on the amount and the price
                    ppa = re.search(r"\((.*?)\)", amount)
                    if ppa:
                        price_per_amount = ppa.group(1)
                        price_per_amount = re.sub(r"""^.*?=\s*|\).*$|[\"'€]""", "", price_per_amount).strip()
                        price_per_amount = re.sub(r"[^\d,.]", "", price_per_amount).replace(",", ".").strip()
                        price_per_amount = round(float(price_per_amount), 2)
                    else:
                        try:
                            amount_calc = int(re.sub(r"""[\"']|\(.*?\)|g(?=\d)|(?<=\d)g""", "", amount).strip())
                            price_per_amount = round((price * (1000/amount_calc)), 2)
                        except:
                            price_per_amount = price
                    
                    amount = re.sub(r"""[\"']|\(.*?\)""", "", amount).strip()
                    
                    ## checks if the product has a bio-label and assigns either True or False to the "bio label" data point of the product
                    biolabel = item.find("div", class_="organicBadge badgeItem search-service-organicBadge search-service-badgeItem")
                    biolabel = True if biolabel else False

                    ## appends all data points of the product to the self.products variable to construct a table of products    
                    self.products.append([product_id, name, amount, price, price_per_amount, offer, biolabel, self.categoy])
                
                logger.info("successfully scraped page %s", self.page)
                self.page += 1
                time.sleep(1) # the rate limiting of the REWE website blocks HTTP requests after roughly 85 requests unless a 1 second buffer is included
            
            except SSLError as e:
                logger.critical(f"SSL error: {e}.")
                sys.exit(1)
            except RequestException as e:
                logger.critical(f"Request failed: {e}.")
                sys.exit(1)
        
        ## executes after all pages were scraped, logs how many pages were scraped, and writes all found products to a csv file
        self.page -= 1
        logger.info("finished scraping. last page: %s. creating csv file...", self.page)
        self.save_to_csv()


    def save_to_csv(self):
        """
        creates a csv file out of all the products saved in the self.products variable
        """
        filename = re.sub(r"^https?://", "", self.url)
        filename = re.sub(r"[/.]", "_", filename) + ".csv"
        with open(os.path.join(data_path, filename), "w") as file:
            writer = csv.writer(file)
            writer.writerows(self.products)
        logger.info("""finished writing csv file %s
              """, filename)


if __name__ == "__main__":
    scrapers = [Scraper(url) for url in websites]
    for instance in scrapers:
        instance.scrape()
    
    end = time.time()
    endprocess = time.process_time()
    
    logger.info(f"""
FINISHED SCRAPING
CHECK VOLUME FOR SCRAPED DATA.
TOTAL CALLS MADE: {calls}
TOTAL ITEMS FOUND: {total_items}
TOTAL RUNTIME: {int((end - start) // 60)} minutes and {int((end - start) % 60)} seconds (precice: {round(end - start, 4)} seconds)
TOTAL CPU RUNTIME: {round(endprocess - startprocess, 2)} seconds
""")
    
    sys.exit(0)
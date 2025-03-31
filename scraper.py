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


def main():
    application = Application()
    application.initiate_scraping()
    application.write_to_database()
    application.stop_program()


class Application:
    """
    creates logs of the programs performance and any potential errors.
    creates the folders for the csv files to be stored.
    creates the environment needed for the Scraper class to work.
    creates the instances of the Scraper classes that scrape the webiste they are assigned to.
    """
    def __init__(self):
        self.startprocess = time.process_time()
        self.start = time.time()
        self.http_calls = 0 ## variables for the Scraper class to track the amount of http requests sent and products scraped, which will be documented in the logs file
        self.total_items = 0
        self.today = datetime.now().strftime('%Y%m%d')
        self.setup_logger()
        self.setup_data_storage()
        self.setup_request_metadata()
        self.setup_scrapers()
        

    def setup_logger(self):
        """creates a logger for monitoring both in the terminal and for reference in a "logs" folder in a subfolder named after the current date"""
        
        logs_path = os.path.join("logs", "scraper")
        os.makedirs(logs_path, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)  

        if not self.logger.handlers:
            formatter = logging.Formatter(fmt="%(asctime)s: %(message)s", datefmt="%Y.%m.%d %H:%M:%S")
            
            ## config for the .log file generated
            log_file = os.path.join(logs_path, f"{datetime.now().strftime('%Y.%m.%d %H-%M-%S')}.log")
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
            ## config for the stream handler that shows logs in the terminal
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(logging.INFO)
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)


    def setup_data_storage(self):
        """creates a "data" folder and a subfolder named after the current date into which the scraped data will be saved"""
        self.logger.info("creating directory for data")
        self.data_path = os.path.join("data", self.today)
        os.makedirs(self.data_path, exist_ok=True)


    def setup_request_metadata(self):
        """creates headers, cookies, and a session for the HTTP requests for the Scraper class"""
        ua = UserAgent()
        headers = {
                "User-Agent": ua.random,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
                "Referer": "https://shop.rewe.de/",
                "Connection": "keep-alive",
                "Cache-Control": "max-age=0"
            }
        
        try:
            with open("store_locations.json", "r") as file:
                self.store_location = json.load(file).get("locations", {}).get("Tussmannstr. 41-63, 40477 Düsseldorf / Pempelfort", None)
                if not self.store_location:
                    self.logger.critical("Store location not found in store_locations.json")
                    raise ValueError
        except FileNotFoundError:
            self.logger.critical("store_locations.json file not found")
            raise FileNotFoundError
        except json.JSONDecodeError:
            self.logger.critical("Invalid JSON format in store_locations.json")
            raise ValueError

        load_dotenv()
        cf_clearance = os.getenv("cf_clearance", "")
        if not cf_clearance:
            self.logger.critical("cf_clearance environment variable is missing")
            raise ValueError
        cookies = {
            "_rdfa": self.store_location,
            "cf_clearance": cf_clearance
            }
        
        self.session = cloudscraper.CloudScraper()
        self.session.headers.update(headers)
        self.session.cookies.update(cookies)


    def setup_scrapers(self):
        """creates instances of the Scraper class based on the amount of URLs in the websites.json file"""
        with open ("websites.json", "r") as file:
            try:
                websites = json.load(file).get("websites")
            except FileNotFoundError:
                self.logger.critical("websites.json file not found")
                raise FileNotFoundError
            except json.JSONDecodeError:
                self.logger.critical("Invalid JSON format in websites.json")
                raise ValueError
            self.scrapers = [Scraper(self, url) for url in websites]


    def initiate_scraping(self):
        for instance in self.scrapers:
            instance.scrape()
            instance.save_to_csv()


    def write_to_database(self):
        pass 


    def stop_program(self):
        if self.session:
            self.session.close()
        self.end = time.time()
        self.endprocess = time.process_time()
        self.logger.info(f"""
            \nFINISHED SCRAPING
            \nCHECK VOLUME FOR SCRAPED DATA.
            \nTOTAL CALLS MADE: {self.http_calls}
            \nTOTAL ITEMS FOUND: {self.total_items}
            \nTOTAL RUNTIME: {int((self.end - self.start) // 60)} minutes and {int((self.end - self.start) % 60)} seconds (precice: {round(self.end - self.start, 4)} seconds)
            \nTOTAL CPU RUNTIME: {round(self.endprocess - self.startprocess, 2)} seconds
            """)
        sys.exit(0)



class Scraper:
    """
    scrapes the website it gets assigned and saves the result to a csv file.
    
    Args:
    string: the URL which it will scrape.
    parent: needs composition from the Application class to function.

    Output: 
    a csv file of all products found on every page of the URL.
    """
    
    def __init__(self, parent, url):
        self.parent = parent
        self.url = url
        self.page = 1 # current page, gets updated at the end of every successfully scraped page
        self.last_page = 1 # during the scrape method, this will get updated to the actual last page if there is more than one page
        self.products_per_page = 250 # the maximum amount of objects that can be shown on a single page is 250
        self.category = re.sub(r"^https?://shop.rewe.de/c/", "", self.url)
        self.products = [
            ["ID", "name", "amount", "price in €","€ per kg", "reduced price", "bio label", "category"]
        ] # the header row for the csv file, structured as a list of lists for constructing a csv file 


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
        
        self.parent.logger.info("""starting to scrape %s""", self.url)
        
        while self.page <= self.last_page: 
            url_page = f"{self.url}/?objectsPerPage={self.products_per_page}&page={self.page}" 
            
            try:
                response = self.parent.session.get(url_page)
                self.parent.logger.debug(f"status code: {response.status_code}")
                self.parent.http_calls += 1
                self.parent.logger.info("successfully reached page %s", self.page)
                soup = BeautifulSoup(response.text, "lxml")
                
                if self.page == 1:
                    self.check_pagination(soup)

                matches = soup.find_all("article")
                for item in matches:
                    self.parent.total_items += 1
                    
                    ## gets the unique product ID to be used as the primary key in the database entry for the product
                    meso_data = item.find("meso-data")
                    if meso_data:
                        data_productid = meso_data.get("data-productid")
                        if data_productid and data_productid.isdigit():  # ensures it's a valid integer string
                            product_id = int(data_productid)
                    if not product_id: # if "meso-data" didn't work, tries to find the "input" element and extract "value"
                        input_element = item.find("input")
                        if input_element:
                            input_value = input_element.get("value")
                            if input_value and input_value.isdigit():  # ensures it's a valid integer string
                                product_id = int(input_value)

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
                    amount = amount.text if amount else "1 Stück"
                    
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
                    amount = re.sub(r"zzgl\..*?Pfand", "", amount).strip()
                    
                    ## checks if the product has a bio-label and assigns either True or False to the "bio label" data point of the product
                    biolabel = item.find("div", class_="organicBadge badgeItem search-service-organicBadge search-service-badgeItem")
                    biolabel = True if biolabel else False

                    ## appends all data points of the product to the self.products variable to construct a table of products
                    self.products.append([product_id, name, amount, price, price_per_amount, offer, biolabel, self.category])
                
                self.parent.logger.info("successfully scraped page %s", self.page)
                self.page += 1
                time.sleep(1) # the rate limiting of the REWE website blocks HTTP requests after roughly 85 requests unless a 1 second buffer is included
            
            except SSLError as e:
                self.parent.logger.critical(f"SSL error: {e}.")
                sys.exit(1)
            except RequestException as e:
                self.parent.logger.critical(f"Request failed: {e}.")
                sys.exit(1)
        
        ## logs how many pages were scraped
        self.page -= 1
        self.parent.logger.info("finished scraping. last page: %s.", self.page)


    def save_to_csv(self):
        """
        creates a csv file out of all the products saved in the self.products variable
        """
        self.parent.logger.info("creating csv file...")
        filename = self.category + ".csv"
        with open(os.path.join(self.parent.data_path, filename), "w") as file:
            writer = csv.writer(file)
            writer.writerows(self.products)
        self.parent.logger.info("""finished writing csv file %s
                """, filename)



if __name__ == "__main__":
    main()
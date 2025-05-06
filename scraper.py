import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from requests.exceptions import SSLError, RequestException
from sqlalchemy.exc import IntegrityError

import db_utils
from models import Categories, Stores, DailyData
from config import LOG_LEVEL


def main():
    application = Application()
    signal.signal(signal.SIGTERM, application.shutdown)
    signal.signal(signal.SIGINT, application.shutdown)
    for scraper in application.scrapers:
        scraper.scrape()
        # scraper.save_as_csv_by_category() # uncomment this line for CSV creation
        # scraper.save_as_single_csv() # uncomment this line for CSV creation
        scraper.write_to_database()
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
        self.http_calls = 0 ## variable for the Scraper class to track the amount of http requests sent, which will be documented in the logs file
        self.total_items = 0 ## variable for the Scraper class to track the amount of products scraped, which will be documented in the logs file
        self.today = datetime.now().date()
        self.setup_logger()
        self.store_locations = self.setup_locations()
        self.scrapers = self.setup_scrapers()
        

    def setup_logger(self):
        """
        creates a logger for monitoring both in the terminal and for reference in 
        a "logs" folder in a subfolder named after the current date.
        """
        
        logs_path = os.path.join("logs", "scraper")
        os.makedirs(logs_path, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(LOG_LEVEL)  

        if not self.logger.handlers:
            formatter = logging.Formatter(fmt="%(asctime)s: %(message)s", datefmt="%Y.%m.%d %H:%M:%S")
            
            ## config for the .log file generated
            file_handler = TimedRotatingFileHandler(
            f"logs/scraper/scraper.log",
            when="midnight",
            backupCount=30
            )
            file_handler.setFormatter(formatter)
            # uncomment the line below for more granular logging config 
            # file_handler.setLevel(logging.INFO)
            self.logger.addHandler(file_handler)
            
            ## config for the stream handler that shows logs in the terminal
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            # uncomment the line below for more granular logging config 
            # stream_handler.setLevel(logging.INFO)
            self.logger.addHandler(stream_handler)


    def setup_data_storage(self, local_storage=True):
        """
        checks for a "data" folder, a database.db file, and a subfolder named after
        the current date into which the scraped data will be saved, and creates them if they do not exist already.
        """

        if local_storage == True:
            data_path = Path("data")
            if not data_path.exists():
                self.logger.info("creating directory for data")
                os.makedirs(data_path)
            
            database_file = Path("data", "bazaar.db")
            if not database_file.exists():
                self.logger.info("creating database")
                database_file.touch()
            
            self.today_data_path = Path("data", "CSVs", self.today.isoformat())
            if not self.today_data_path.exists():
                self.logger.info("creating data folder for today")
                os.makedirs(self.today_data_path, exist_ok=True)


    def setup_locations(self):
        """
        gets all key-value pairs in the store_locations.json file, which includes the address of
        a store and the session cookie to load the store-specific data on the website.
        """

        try:
            with open("store_locations.json", "r") as file:
                store_locations = json.load(file).get("locations", None)
                if not store_locations:
                    self.logger.critical("Store location not found in store_locations.json")
                    raise ValueError
                return store_locations
        
        except FileNotFoundError:
            self.logger.critical("store_locations.json file not found")
            raise FileNotFoundError
        except json.JSONDecodeError:
            self.logger.critical("Invalid JSON format in store_locations.json")
            raise ValueError


    def setup_scrapers(self):
        """
        creates instances of the Scraper class based on the amount of URLs in the websites.json file.
        """

        with open ("websites.json", "r") as file:
            try:
                websites = json.load(file).get("websites")
            
            except FileNotFoundError:
                self.logger.critical("websites.json file not found")
                raise FileNotFoundError
            except json.JSONDecodeError:
                self.logger.critical("Invalid JSON format in websites.json")
                raise ValueError

        scrapers = [Scraper(self, websites, location, location_cookie) for location, location_cookie in self.store_locations.items()]
        return scrapers


    def stop_program(self, success=True):
        """
        logs the amount of http calls made, the total amount of items found, the total runtime, and total CPU runtime. 
        Exits the program.
        """

        self.end = time.time()
        self.endprocess = time.process_time()
        if success:
            self.logger.info(f"""
                \nFINISHED SCRAPING.
                \nCHECK VOLUME FOR SCRAPED DATA.
                \nTOTAL CALLS MADE: {self.http_calls}
                \nTOTAL ITEMS FOUND: {self.total_items}
                \nTOTAL RUNTIME: {int((self.end - self.start) // 60)} minutes and {int((self.end - self.start) % 60)} seconds (precice: {round(self.end - self.start, 4)} seconds)
                \nTOTAL CPU RUNTIME: {round(self.endprocess - self.startprocess, 2)} seconds
                """)
        else: 
            self.logger.error(f"""
                \nSCRAPING UNSUCCESSFUL.
                \nCHECK LOGS FOR ERROR CODES.
                \nTOTAL CALLS MADE: {self.http_calls}
                \nTOTAL ITEMS FOUND: {self.total_items}
                \nTOTAL RUNTIME: {int((self.end - self.start) // 60)} minutes and {int((self.end - self.start) % 60)} seconds (precice: {round(self.end - self.start, 4)} seconds)
                \nTOTAL CPU RUNTIME: {round(self.endprocess - self.startprocess, 2)} seconds
                """)
        sys.exit(0)


    def shutdown(self, signum, frame):
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop_program(success=False)


class Scraper:
    """
    scrapes all websites it gets assigned and saves the result as a csv files and/or as a database entry.
    For every store location in the store_locations.json file, one Scraper will be created.
    
    Args:
    parent: needs composition with the Application class to function.
    websites: list of strings of the URLs which it will scrape.
    location: dictionary containing a key-value pair of location address and corresponding cookie value from the store_locations.json file

    Output: 
    depending on which functions are called at the beginning of the script:
        save_as_csv_by_category: mutliple csv files for every category of product.
        save_as_single_csv: a single csv file of all products of the store.
        write_to_database: a database entry into the relational database.
    """
    
    def __init__(self, parent, websites, location, location_cookie):
        self.parent = parent
        self.websites = websites
        self.location = location
        self.location_cookie = location_cookie
        self.session = self.setup_request_session()
        self.products_per_page = 250 # the maximum amount of objects that can be shown on a single webpage on the REWE website is 250
        self.all_products = {} # placeholder for the dictionary holding the dataframe structures that will in turn hold all scraped products. Will be used for saving as CSV files or writing to a relational database
        self.failed_attempts = 0


    def setup_request_session(self):
        """
        creates a session with headers (that lower chance of bot detection) and 
        cookies (that informs the REWE website which store's products to show) for the HTTP requests.
        """

        ua = UserAgent()
        headers = {
                "User-Agent": ua.random,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
                "Referer": "https://shop.rewe.de/",
                "Connection": "keep-alive",
                "Cache-Control": "max-age=0"
            }
        
        cookies = {
            "_rdfa": self.location_cookie,
            }
        
        session = cloudscraper.CloudScraper()
        session.headers.update(headers)
        session.cookies.update(cookies)
        return session


    def check_pagination(self, soup):
        """
        checks for pagination in the BeautifulSoup object and updates the self.last_page variable accordingly
        
        args:
        soup: BeautifulSoup Class
        
        output:
        either the last page of the website or 1 (meaning there is only one page)
        """

        lastpage = soup.find_all("button", class_="PostRequestGetFormButton paginationPage paginationPageLink")
        if lastpage:
            last_page = int(lastpage[-1].get_text(strip=True))
            return last_page
        else:
            return 1


    def parse_amount(self, amount):
        """
        takes the amount scraped from the website and parses it for mentions of any typically 
        found standard units like gramm or liter and extracts the listed amount and unit accordingly.

        Args:
        amount: a string derived from the BeautifulSoup item that mentions the amount of a product.

        Output:
        listed_amount: a float that only lists the amount of the product.
        listed_unit: a string that only lists the unit of the product.
        """

        amount_cleaned = re.sub(r"""[\"']|\(.*?\)""", "", amount).strip()
        amount_cleaned = re.sub(r"\s+", "", amount_cleaned)
        listed_amount = None
        listed_unit = None
        units = ["g", "ml", "kg", "l"]
        
        for unit in units:
            try:
                search_unit = re.search(rf"(\d+)(?={unit})", amount_cleaned)
                if search_unit:
                    if "," in amount_cleaned:
                        listed_amount = re.search(r"\d+(\,\d+)?", amount_cleaned)
                        listed_amount = float(listed_amount.group().replace(",", "."))
                    else:
                        listed_amount = float(search_unit.group(1))
                    listed_unit = unit
            except:
                pass

        if not listed_amount:
            listed_amount = 1.0
            listed_unit = "piece"
            
        try:
            multiplier = re.search(r"\d+(?=x)", amount_cleaned).group()
            if multiplier:
                listed_amount = float(multiplier) * float(listed_amount)
        except:
            pass

        return listed_amount, listed_unit


    def scrape(self):
        """
        scrapes the products of every website listed in the websites.json file

        output:
        a dictionary of pandas dataframes structured after the DailyData ORM in the models.py script
        """

        for website in self.websites:
            page = 1 # current page, gets updated at the end of every successfully scraped page
            last_page = 1 # during the scrape method, this will get updated to the actual last page if there is more than one page
            category = re.sub(r"^https?://shop.rewe.de/c/", "", website)
            products = []
            self.parent.logger.info("""starting to scrape %s""", website)
        
            while page <= last_page: 
                url_page = f"{website}/?objectsPerPage={self.products_per_page}&page={page}" 
                
                try:
                    response = self.session.get(url_page)
                    self.parent.logger.debug(f"status code: {response.status_code}")
                    self.parent.http_calls += 1
                    self.parent.logger.info("successfully reached page %s", page)
                    soup = BeautifulSoup(response.text, "lxml")
                    
                    ## for debugging: saves the recieved HTML as a file in a folder called "workbench" for reference
                    if self.parent.logger.isEnabledFor(logging.DEBUG):
                        os.makedirs("workbench", exist_ok=True)
                        with open(f"workbench/soup_html_{self.parent.today}.html", "w") as file:
                            file.write(soup.prettify())

                    ## this will only run once unless a last page higher than 1 is found
                    if page == 1:
                        last_page = self.check_pagination(soup)

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
                                    product_id = int(input_value) if product_id else None

                        ## gets the name of the product and cleans the data point using regular expressions
                        name = item.find("div", class_="LinesEllipsis")
                        name = name.text if name else None
                        name = re.sub(r"""[\"']""", "", name).strip() if name else None
                        
                        ## gets the price of the product and turns it into an integer
                        ## checks if the product has a reduced price and assigns either True or False to the "reduced price" data point of the product
                        listed_price = item.find("div", class_="search-service-productPrice productPrice") 
                        if listed_price:
                            is_on_offer = False
                            listed_price = listed_price.text
                            listed_price = float(listed_price.replace("€", "").replace(",",".").strip())
                        else:
                            is_on_offer = True
                            listed_price = item.find("div", class_="search-service-productOfferPrice productOfferPrice")
                            listed_price = listed_price.text
                            listed_price = float(listed_price.replace("€", "").replace(",",".").strip())
                        
                        ## gets the listed amount of the product and its unit measurement
                        listed_amount = item.find("div", class_="productGrammage search-service-productGrammage")
                        listed_amount = listed_amount.text if listed_amount else "1 Stück"
                        listed_amount, listed_unit = self.parse_amount(listed_amount)                        
                        
                        ## checks if the product has a bio-label and assigns either True or False to the "bio label" data point of the product
                        biolabel = item.find("div", class_="organicBadge badgeItem search-service-organicBadge search-service-badgeItem")
                        biolabel = True if biolabel else False

                        ## appends all data points of the product to the self.products variable to construct a table of products
                        products.append({"date": self.parent.today,
                                         "store_id": self.location,
                                         "product_id": product_id, 
                                         "product_name": name, 
                                         "has_bio_label": biolabel, 
                                         "category_id": category, 
                                         "listed_price": listed_price, 
                                         "listed_amount": listed_amount, 
                                         "listed_unit": listed_unit, 
                                         "is_on_offer": is_on_offer, 
                                         })
                    
                    self.parent.logger.info("successfully scraped page %s", page)
                    page += 1
                    time.sleep(1) # the rate limiting of the REWE website blocks HTTP requests after roughly 85 requests unless a 1 second buffer is included
                
                except SSLError as e:
                    self.parent.logger.critical(f"SSL error: {e}.")
                    self.parent.logger.info(f"will retry in 10 seconds")
                    self.failed_attempts += 1
                    if self.failed_attempts > 5:
                        self.parent.logger.critical(f"maximum attempts reached. Quitting program.")
                        self.parent.stop_program(success=False)
                    else:
                        time.sleep(10)
                        self.scrape()
                
                except RequestException as e:
                    self.parent.logger.critical(f"Request failed: {e}.")
                    self.parent.logger.info(f"will retry in 10 seconds")
                    self.failed_attempts += 1
                    if self.failed_attempts > 5:
                        self.parent.logger.critical(f"maximum attempts reached. Quitting program.")
                        self.parent.stop_program(success=False)
                    else:
                        time.sleep(10)
                        self.scrape()
            
            ## creates a Pandas dataframe out of all the gathered products and stores 
            ## it in the list variable called "self.all_products"
            df = pd.DataFrame(products)
            df.fillna(0)
            df.set_index("product_id")
            df.drop_duplicates(subset="product_id", inplace=True)
            self.all_products[category] = df
            
            page -= 1
            self.parent.logger.info("finished scraping. last page: %s.", page)
        
        if self.session:
            self.session.close()


    def save_as_csv_by_category(self):
        """
        creates csv files for each category listed in the websites.json file out of
        all the products saved in the self.all_products variable.
        """

        self.parent.setup_data_storage()
        self.parent.logger.info("creating csv files...")
        
        for category, dataframe in self.all_products.items():
            filename = os.path.join(self.parent.today_data_path, f"{category}.csv")
            dataframe.drop("category_ID", axis=1)
            dataframe.to_csv(filename)

            self.parent.logger.info("""finished writing csv file %s""", filename)


    def save_as_single_csv(self):
        """
        creates a single csv file out of all the products saved in the self.all_products variable.
        """

        self.parent.setup_data_storage()
        self.parent.logger.info("creating csv file...")

        dataframe = pd.concat(self.all_products.values())
        filename = os.path.join(self.parent.today_data_path, f"{self.parent.today}.csv")
        dataframe.to_csv(filename)
        
        self.parent.logger.info("""finished writing csv file %s""", filename)


    def write_to_database(self):
        """
        writes all products in the the self.all_products variable to the DailyData table in 
        the database. Cleans up the data before insertion to be in line with the database ORM schema.
        """

        self.parent.logger.info("preparing data for insertion into DailyData table in database...")
        dataframe = pd.concat(self.all_products.values())
        dataframe.drop_duplicates(subset="product_id", inplace=True)
        data = dataframe.to_dict(orient="records")

        with db_utils.session_query() as session:
            ## changes the categories from its names to the corresponding category_id
            category_query = session.query(Categories).all()
            category_mapping = {category.category_name: category.category_id for category in category_query}
            for product in data:
                category = product["category_id"]
                if category in category_mapping:
                    product["category_id"] = category_mapping[category]

            ## changes the stores from its names to the corresponding store_id
            store_query = session.query(Stores).all()
            store_mapping = {store.store_name: store.store_id for store in store_query}
            for product in data:
                store = product["store_id"]
                if store in store_mapping:
                    product["store_id"] = store_mapping[store]

        self.parent.logger.info("writing to DailyData table in database...")
        try:
            with db_utils.session_commit() as session:
                session.bulk_insert_mappings(DailyData, data)

        except IntegrityError:
            with db_utils.session_commit() as session:
                db_utils.bulk_upsert(DailyData, data)

        finally:
            try:
                subprocess.run([sys.executable, "DailyData_handler.py"])
            except Exception as e:
                self.parent.logger.error(f"an error ocurred while creating statistical data: {e}")


if __name__ == "__main__":
    main()
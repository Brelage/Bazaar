import csv
import os
import time
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

os.makedirs("logs/scraper", exist_ok=True)
logger = logging.getLogger(__name__)
logging.basicConfig(
    handlers= [
        logging.FileHandler(f"logs/scraper/{datetime.now().strftime("%Y.%m.%d-%H-%M-%S")}.log"),
        logging.StreamHandler()
    ],
    level=logging.INFO,
    format="%(asctime)s: %(message)s",
    datefmt="%Y.%m.%d %H:%M:%S"
    )

websites = [
    "https://shop.rewe.de/c/obst-gemuese",
    "https://shop.rewe.de/c/haus-freizeit",
    "https://shop.rewe.de/c/kueche-haushalt",
    "https://shop.rewe.de/c/tierbedarf",
    "https://shop.rewe.de/c/babybedarf",
    "https://shop.rewe.de/c/drogerie-kosmetik",
    "https://shop.rewe.de/c/getraenke-genussmittel",
    "https://shop.rewe.de/c/kaffee-tee-kakao",
    "https://shop.rewe.de/c/suesses-salziges",
    "https://shop.rewe.de/c/fertiggerichte-konserven",
    "https://shop.rewe.de/c/oele-sossen-gewuerze",
    "https://shop.rewe.de/c/kochen-backen",
    "https://shop.rewe.de/c/brot-cerealien-aufstriche",
    "https://shop.rewe.de/c/tiefkuehlkost",
    "https://shop.rewe.de/c/kaese-eier-molkerei",
    "https://shop.rewe.de/c/fleisch-fisch"
]
ua = UserAgent()
headers = {
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://shop.rewe.de/",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0"
    }

load_dotenv()
cookies = {
     "_rdfa": os.getenv("_rdfa", ""),
     "cf_clearance": os.getenv("cf_clearance", "")
    }
if not cookies["_rdfa"] or not cookies["cf_clearance"]:
    raise ValueError("Missing required cookies. Check environment variables.")

session = cloudscraper.CloudScraper()
calls = 0
total_items = 0

logger.info("creating directory for data")
folder_name = f"data/{datetime.now().strftime('%Y%m%d')}"
os.makedirs(folder_name, exist_ok=True)


class Scraper:
    def __init__(self, url):
        self.url = url
        self.page = 1
        self.last_page = 1
        self.products = [
            ["name", "amount", "price in €","€ per kg", "reduced price", "bio label"]
        ]

    def scrape(self):
        logger.info("""
starting to scrape %s""", self.url)
        while self.page <= self.last_page: 
            url_page = f"{self.url}/?objectsPerPage=80&page={self.page}"
            try:
                response = session.get(url_page, cookies=cookies, headers=headers)
                logger.debug(f"status code: {response.status_code}")
                global calls
                calls += 1
                logger.info("successfully reached page %s", self.page)
                soup = BeautifulSoup(response.text, "lxml")
                
                if self.page == 1:
                    lastpage = soup.find_all("button", class_="PostRequestGetFormButton paginationPage paginationPageLink")
                    if lastpage:
                        self.last_page = int(lastpage[-1].get_text(strip=True))

                matches = soup.find_all("div", class_="search-service-productDetailsWrapper productDetailsWrapper")
                for item in matches:
                    global total_items
                    total_items += 1
                    
                    name = item.find("div", class_="LinesEllipsis")
                    name = name.text if name else ""
                    name = re.sub(r"""[\"']""", "", name).strip()
                    
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
                    
                    amount = item.find("div", class_="productGrammage search-service-productGrammage")
                    amount = amount.text if amount else ""
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
                            amount = re.sub(r"""[\"']|\(.*?\)""", "", amount).strip()
                            price_per_amount = price
                    
                    biolabel = item.find("div", class_="organicBadge badgeItem search-service-organicBadge search-service-badgeItem")
                    biolabel = True if biolabel else False
                        
                    self.products.append([name, amount, price, price_per_amount, offer, biolabel])
                logger.info("successfully scraped page %s", self.page)
                self.page += 1
                time.sleep(1)
            except SSLError as e:
                logger.critical(f"SSL error: {e}.")
                sys.exit(1)
            except RequestException as e:
                logger.critical(f"Request failed: {e}.")
                sys.exit(1)
        else:
            self.page -= 1
            logger.info("finished scraping. last page: %s. creating csv file...", self.page)
            self.save_to_csv()


    def save_to_csv(self):
        filename = re.sub(r"^https?://", " ", self.url)
        filename = re.sub(r"[/.]", "_", filename)
        with open(f"{folder_name}/{filename}.csv", "w") as file:
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
TOTAL RUNTIME: {round(end - start, 4)} seconds
TOTAL CPU RUNTIME: {round(endprocess - startprocess, 4)} seconds
""")
    sys.exit(0)
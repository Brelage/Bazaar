import requests
import aiohttp
import json
import csv
import time
import re
import os
import pandas as pd
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from requests.exceptions import SSLError, RequestException


with open("websites.json", "r") as file:
    websites = json.load(file).get("websites", [])

with open("headers.json", "r") as file:
    headers = json.load(file).get("headers", {})

load_dotenv()
cookies = {
     "_rdfa": os.getenv("_rdfa", ""),
     "cf_clearance": os.getenv("cf_clearance", "")
    }
if not cookies["_rdfa"] or not cookies["cf_clearance"]:
    raise ValueError("Missing required cookies. Check environment variables.")


session = requests.Session()

class Scraper:
    def __init__(self, url):
        self.url = url
        self.page = 1
        self.products = [
            ["name", "amount", "price (in €)","price per amount", "reduced price", "bio label"]
        ]

    def scrape(self):
        while True:
            url_page = f"{self.url}/?objectsPerPage=80&page={self.page}"
            try:
                response = session.get(url_page, cookies=cookies, headers=headers)
                if response.status_code < 400:
                    soup = BeautifulSoup(response.text, "lxml")
                    matches = soup.find_all("div", class_="search-service-productDetailsWrapper productDetailsWrapper")
                                       
                    for item in matches:
                        name = item.find("div", class_="LinesEllipsis")
                        name = name.text if name else ""
                        name = re.sub(r"""[\"']""", "", name, flags=re.IGNORECASE).strip()
                        
                        amount = item.find("div", class_="productGrammage search-service-productGrammage")
                        amount = amount.text if amount else ""
                        ppa = re.search(r"\((.*?)\)", amount)
                        if ppa:
                            price_per_amount = ppa.group(1)
                            price_per_amount = re.sub(r"""[\"'€]""", "", amount).strip()
                        else:
                            price_per_amount = None
                        amount = re.sub(r"""[\"']|\(.*?\)""", "", amount).strip()
                        

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

                        biolabel = item.find("div", class_="organicBadge badgeItem search-service-organicBadge search-service-badgeItem")
                        biolabel = True if biolabel else False
                        
                        self.products.append([name, amount, price, price_per_amount, offer, biolabel])
                    self.page+=1
                    time.sleep(1)
                else:
                    print(f"""
Finished {self.url}. 
Last Page: {self.page-1}.
                          """)
                    break
            except SSLError as e:
                print(f"SSL error: {e}")
                time.sleep(5)
            except RequestException as e:
                print(f"Request failed: {e}")
                return None

        self.save_to_csv()

    def save_to_csv(self):
        filename = re.sub(r"^https?://", " ", self.url)
        filename = re.sub(r"[/.]", "_", filename)
        with open(f"data/{filename}.csv", "w") as file:
            writer = csv.writer(file)
            writer.writerows(self.products)


for url in websites:
    scraper = Scraper(url)
    scraper.scrape()

print("""
FINISHED SCRAPING!
CHECK DATA FOLDER FOR RESULTS!
      """)
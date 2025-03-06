import requests
import aiohttp
from bs4 import BeautifulSoup


class Scraper:
    def __init__(self, url):
        self.url = url
        self.session = requests.Session(self.url)
    
    def scrape(self):
        self.response = requests.get(self.url)
        soup = BeautifulSoup(response.text, 'XXXXXX PLACEHOLDER XXXXXXX')
        
        headers = {"Accept-Encoding": "gzip"}
        with requests.get(self.url, headers=headers, stream=True) as response:
            for chunk in response.iter_content(chunk_size=1024):
                if b"<body>" in chunk:
                    html_body = chunk.decode("utf-8")
                    break




class Initializer:
    def __init__(self, websites):
        self.websites = websites
        pass


sitemap_url = "https://www.rewe.de/sitemaps/sitemap.xml"
response = requests.get(sitemap_url)
soup = BeautifulSoup(response.content, "xml")

urls = [loc.text for loc in soup.find_all("loc")]




if __name__ == "__main__":
    try:
        initializer = Initializer("websites.json")
    except Exception as e:
        raise
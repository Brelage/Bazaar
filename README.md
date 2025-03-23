# Bazaar
Scraper of supermarket product data using BeautifulSoup for parsing. 
Will scrape any websites that are listed in the script.
In its current state, this program is built with the REWE supermarket website as reference.


## Features
- The script creates csv files that have the following columns: name, amount, price per item, price per kilogramm, whether the product currently has a reduced price, whether the product has a bio label. The scraped data is cleaned using regular expressions for easier data processing further down in the pipeline.
- The script checks for pagination of the website in its first http request and iterates over the pages according to the amount of existing pages. This avoids unnecessary http requests and errors while scraping.
- The script creates logs to track runtime, CPU usage time, amount of sites scraped, and amount of products found.
- The script bypasses Cloudflare javascript blocking by using the cloudscraper library. It preloads randomized User-Agents, headers, and cookies for HTTP-Requests to bypass Cloudflare bot detection. The requests to the websites usually reach a cloudflareBotScore (a score from 1 to 99 that indicates how likely that request came from a bot) above 90. According to Cloudflare, "a score of 1 means Cloudflare is quite certain the request was automated, while a score of 99 means Cloudflare is quite certain the request came from a human".
- as testing has shown, the fairly robust anti-detection measures also enable this script to run inside a docker container and remain undetected, allowing for containerized deployment.


## Setup
1. use git clone to clone this repository into an IDE:

```
git clone https://github.com/Brelage/Bazaar
```

2. install dependencies

```
pip install -r requirements.txt
```

3. create a .env file: When opening any of the webpages of the REWE supermarket in a browser, a pop-up window shows up asking for a postcode to find the closest supermarket. In order to bypass the pop-up window, two cookies need to be sent along with the http request: "_rdfa" and "cf_clearance". In its current form, the script fetches these cookies from a .env file in order to make functioning queries. Since automated cookie generation isn't implemented yet, you need to extract these two cookies from your own browser session and save them in a .env file (see the .env.example file as reference). 


## Planned features 
- automated data analysis: The csv files created through the scraper script are planned to form the basis of a database to track changes in the available products of supermarkets. Automated daily scraping combined with automated data analysis could allow for automated findings of trends in the products. 
- automatic cookie generation: In its current form, the script only has one location-cookie preloaded, meaning it only checks the products of one supermarket. In order to create a more holistic database, automatic cookie generation is planned as a feature in the future.
- concurrency: currently, the script goes through one webpage at a time and stops for one second after each one. This enables the script to avoid a 429 "too many requests" HTTP status code, but it also makes the script quite slow. Implementation of concurrency could reduce runtime to be a 10th of its current runtime or less, but would require a lot more resources to avoid bot detection. One possible way of implementing this could be through a headless browser like selenium. 
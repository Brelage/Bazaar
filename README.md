# Bazaar
Web scraping project that tracks data points such as prices and offers of supermarket products over time and integrates data collection, storage, and analysis. It uses an object-oriented approach, scrapes websites using BeautifulSoup, and uses SQLAlchemy for database management. The project can be containerized with Docker.

In its current state, this program is built with the REWE supermarket website as reference.


## Features
- The program creates pandas DataFrames, which in turn can either be used to write to a relational database or create csv files that have the following columns: name, amount, price per item, price per kilogramm, whether the product currently has a reduced price, whether the product has a bio label, which category it belongs to. The scraped data is cleaned using regular expressions for easier data processing further down in the pipeline.
- The program has a modular, object-oriented structure, making it easy for scaling. Depending on how the store_locations.json and websites.json files are configured, this program will scrape all products (or a specific subset) of all stores declared in the store_locations.json file.
- The program checks for pagination of the website in its first http request and iterates over the pages according to the amount of existing pages. This avoids unnecessary http requests and errors while scraping.
- The program creates logs to track runtime, CPU usage time, amount of sites scraped, and amount of products found.
- The program bypasses Cloudflare javascript blocking by using the cloudscraper library. It preloads randomized User-Agents, headers, and cookies for HTTP-Requests to bypass Cloudflare bot detection. The requests to the websites usually reach a cloudflareBotScore (a score from 1 to 99 that indicates how likely that request came from a bot) above 90. According to Cloudflare, "a score of 1 means Cloudflare is quite certain the request was automated, while a score of 99 means Cloudflare is quite certain the request came from a human".
- as testing has shown, the fairly robust anti-detection measures also enable this program to run inside a docker container and remain undetected, allowing for containerized deployment.


## Setup
1. use git clone to clone this repository into an IDE:

```
git clone https://github.com/Brelage/Bazaar
```

2. install dependencies

```
pip install -r requirements.txt
```

3. store session cookies in store_locations.json file: 
When opening any of the webpages of the REWE supermarket in a browser, a pop-up window shows up asking for a postcode to find the closest supermarket. In order to inform the Scraper which supermarket to scrape, a cookie with the name "_rdfa" needs to be sent along with the http request, with _rdfa being a session cookie that informs the website about the desired store. 
Since automated cookie generation isn't implemented yet, you need to extract this cookie from your own browser session after you have selected a store location and save it in the the store_locations.json file (see the store_locations_example.json file as reference). 

4. initialise the database using Alembic.
For this, a .env file needs to be configured with the URL to the database. 
The .env.example file is configured for an implementation of this program using SQLite. It can be used as a reference. If another type of Database Management System like MySQL or PostgreSQL is intended to be used for this program, then adjust accordingly.

In order for the program to run properly, a database needs to be initialised, preferably using Alembic. For this, simply run the following commands:
```
alembic revision --autogenerate -m "database initialisation"
```
```
alembic upgrade head
```

5. (optional) configure the websites.json file to only scrape certain categories of the supermarket


## Database schema
This program was written with SQLAlchemy for an agnostic approach to Database Management Systems. It uses Alembic for database initiation and migration.
the structure of the schema is as follows:
- Stores: tracks which stores are being tracked.
- Categories: tracks the different categories that a product can have.
- Products: tracks all products, meaning every product offered by REWE. 
    - Depending on the store, prices, availability etc. might vary, which is why this table only stores static data about the products.
- ProductObservations: this tracks all changes to products across supermarkets.
    - Tracks price changes, whether a product is on offer, and whether the product is still available.
    - This table provides the data to create historical analysis of any product.
- DailyStatistics: tracks daily metadata about all stores being tracked. 
    - Metadata includes:
        - the mean price and the median price of every category for the day
        - the amount of products with a bio-label 
        - the amount of products that are currently on offer
    - data in this table is based on the calculations made by the parser.py script.


## Planned features 
- automated data analysis: The data stored in the database is meant to form the basis of statistical analysis. Automated daily scraping combined with automated data analysis could allow for automated findings of trends in the products.
- automatic cookie generation: In its current form, the script only scrapes the stores that are listed in the store_locations.json file. In order to improve scalability and enable a more holistic database, automatic cookie generation is planned as a feature in the future.
- concurrency: currently, the script goes through one webpage at a time and stops for one second after each one. This enables the script to avoid a 429 "too many requests" HTTP status code, but it also makes the script quite slow. Implementation of concurrency could reduce runtime to be a 10th of its current runtime or less, but would require a lot more resources to avoid bot detection. One possible way of implementing this could be through a headless browser like Selenium. 


## LLM usage declaration
This program was entirely manually written and not generated by an AI. Vanilla VS Code without Copilot was used as the IDE for this project. LLMs were used only for consultations and recommendations on the broader system architecture alongside independent research to verify approaches. Recommendations made by LLMs were critically probed before being considered for the program. No code was implemented without prior testing. 
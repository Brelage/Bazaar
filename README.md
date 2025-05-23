# Bazaar
Bazaar tracks data points such as prices and offers of supermarket products over time and integrates data collection, storage, and analysis. It uses an object-oriented approach, scrapes websites using BeautifulSoup, and uses SQLAlchemy for database management. The project can be containerized with Docker, files for quick containerised deployment are also in the repository. Bazaar collects, cleans, stores, and labels its data to allow for data-driven decision making. Ultimately, the user can use Bazaar to make informed decisions about which store to visit, which products are best for them, and what changes can be seen across stores.
Bazaar consists of a webscraping automation script and a database back-end. A database visualisation front-end is planned as well.

In its current state, this program is built with the REWE supermarket website as reference.


## Features
- The program creates pandas DataFrames, which in turn can either be used to write to a relational database or create csv files. The scraped data is cleaned using regular expressions for easier data processing further down in the pipeline.
- The program has a modular, object-oriented structure, making it easy for scaling. Depending on how the config file is configured, this program will scrape all products (or a specific subset) of all declared stores.
- The program checks for pagination of the website in its first http request and iterates over the pages according to the amount of existing pages. This avoids unnecessary http requests and errors while scraping.
- The program creates logs to track runtime, CPU usage time, amount of sites scraped, and amount of products found.
- The program bypasses Cloudflare javascript blocking by using the cloudscraper library. It preloads randomized User-Agents, headers, and cookies for HTTP-Requests to bypass Cloudflare bot detection. The requests to the websites usually reach a cloudflareBotScore (a score from 1 to 99 that indicates how likely that request came from a bot) above 90. According to Cloudflare, "a score of 1 means Cloudflare is quite certain the request was automated, while a score of 99 means Cloudflare is quite certain the request came from a human".
- as testing has shown, the fairly robust anti-detection measures also enable this program to run inside a docker container and remain undetected, allowing for containerized deployment.


## Database schema
The data structure for Bazaar is a data warehouse (OLAP), using a snowflake design with highly normalized tables. The data tracked with Bazaar allows for time-series analysis.
This program was written with SQLAlchemy for an entirely agnostic approach to Database Management Systems. It uses Alembic for database initiation and migration.
the structure of the schema is as follows:
- DailyData: this is the entry point for the results of the webscraper. The data in this table is used to check for daily changes, which will get documented in the ProductObservations table. 
- Stores: tracks which stores are being tracked.
- Categories: tracks the different categories that a product can have.
- Products: tracks all products, meaning every product offered by REWE. 
    - Depending on the store, prices, availability etc. might vary, which is why this table only stores static data about the products.
- ProductObservations: this tracks all changes to products across supermarkets.
    - Tracks price or amount changes, whether a product is on offer, and whether the product is still available.
    - This table provides the data to create historical analysis of any product.
- DailyStatistics: tracks daily statistics about all stores being tracked (for details on datapoints, check models.py file). 
- CategoryStatistics: tracks the same statistics as DailyStatistics, but on a per-category basis for more granular, actionable data (for details on datapoints, check models.py file).

Below is a graphical representation of the database schema and the relationships between the tables:
![Database Schema](https://i.imgur.com/k7Ou5en.png)


## Setup
1. use git clone to clone this repository into an IDE:

```
git clone https://github.com/Brelage/Bazaar
```

2. install dependencies

```
pip install -r requirements.txt
```

3. store session cookies in the config file: 
When opening any of the webpages of the REWE supermarket in a browser, a pop-up window shows up asking for a postcode to find the closest supermarket. In order to inform the Scraper which supermarket to scrape, a cookie with the name "_rdfa" needs to be sent along with the http request, with _rdfa being a session cookie that informs the website about the desired store. 
Since automated cookie generation isn't implemented yet, you need to extract this cookie from your own browser session after you have selected a store location and save it in the the config file in the LOCATIONS variable. The structure of LOCATIONS is a Python dictionary where the key is the store's location and the value is the session cookie associated with that location. 

1. initialise the database using Alembic.
For this, the config file needs to be configured with the URL to the database. 
By default, the config file is configured for an implementation of this program using SQLite. If a SQLite database is fine, then no changes need to be made. If another type of Database Management System like MySQL or PostgreSQL is intended to be used for this program, then adjust the config file and the database_engine.py script accordingly, passing in credentials. Alternatively, a .env file or an external secrets manager can be used as well, though this would require more adjustments.

In order for the program to run properly, a database needs to be initialised, preferably using Alembic. For this, simply run the following commands:
```
alembic revision --autogenerate -m "database initialisation"
```
```
alembic upgrade head
```


## Planned features 
- automatic cookie generation: In its current form, the script only scrapes the stores that are listed in the config file. In order to improve scalability and enable a more holistic database, automatic cookie generation is planned as a feature in the future.
- concurrency: currently, the script goes through one webpage at a time and stops for one second after each one. This enables the script to avoid a 429 "too many requests" HTTP status code, but it also makes the script quite slow. Implementation of concurrency could reduce runtime to be a 10th of its current runtime or less, but would require a lot more resources to avoid bot detection. One possible way of implementing this could be through a headless browser like Selenium. 
- data visualisation: the database can build the backend for an interactive data visualisation architecture. The planned tech stack for this feature is plotly for graph creation + dash for interactive GUI + flask for web deployment.


## LLM usage declaration
This program was entirely manually written and not generated by an AI. Vanilla VS Code without Copilot was used as the IDE for this project. LLMs were used only for consultations and recommendations on the broader system architecture alongside independent research to verify approaches. Recommendations made by LLMs were critically probed before being considered for the program. No code was implemented without prior testing. 
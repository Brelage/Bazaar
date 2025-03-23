# Bazaar
Scraper of supermarket product data using BeautifulSoup for parsing. 
Will scrape any websites that are listed in the script.
In its current state, this program is built with the REWE supermarket website as reference.

## Features
- The script creates csv files that have the following columns: name, amount, price per item, price per kilogramm, whether the product currently has a reduced price, whether the product has a bio label. The scraped data is cleaned using regular expressions for easier data processing further down in the pipeline.
- The script checks for pagination of the website in its first http request and iterates over the pages according to the amount of existing pages. This avoids unnecessary http requests and errors while scraping.
- The script creates logs to track runtime, CPU usage time, amount of sites scraped, amount of products found.
- The script bypasses Cloudflare javascript blocking by using the cloudscraper library. It preloads randomized User-Agents, headers, and cookies for HTTP-Requests to bypass Cloudflare bot detection. The requests to the websites usually reach a cloudflareBotScore, a score from 1 to 99 that indicates how likely that request came from a bot, above 90. A score of 1 means Cloudflare is quite certain the request was automated, while a score of 99 means Cloudflare is quite certain the request came from a human.
- as testing has shown, the fairly robust anti-detection measures also enable this script to run inside a docker container and remain undetected, allowing for containerized deployment.

## Planned features 
- automated data analysis: The csv files created through the scraper script are meant to form the basis of a database to track changes in the available products of supermarkets. Automated daily scraping combined with automated data analysis could allow for automated findings of trends in the products. 
- automatic cookie generation: In its current form, the script only has one location-cookie preloaded, meaning it only checks the products of one supermarket. In order to create a more holistic database, automatic cookie generation is planned as a feature in the future.
- concurrency: currently, the script goes through one webpage at a time and stops for one second after each one. This enables the script to avoid a 429 "too many requests" HTTP status code, but it also makes the script quite slow. Implementation of concurrency could reduce runtime to be a 10th of its current runtime or less, but would require a lot more resources to avoid bot detection. One possible way of implementing this could be through a headless browser like selenium. 
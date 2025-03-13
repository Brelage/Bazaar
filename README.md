# Bazaar
Scraper of supermarket product data. 
Will scrape any websites that are listed in the websites.json file (see websites-example.json for reference).

The output are csv files that have the name, amount, price, and price per amount listed as well as boolean values on whether the product currently has a reduced price and a bio label. 

In its current state, this program is built with the REWE supermarket website as reference. 

So far, the script has a crude way of brute forcing pagination, which will replaced by an approach that is more conscious of the webpage structure itself.

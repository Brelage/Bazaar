import os 
import sys
import time
import logging
import seaborn
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from datetime import datetime
from database_engine import SessionLocal
from models import DailyStatistics, Categories, Stores, ProductObservations, DailyData

"""
The default behavior of this script is to create a graph depicting the 
distribution of price of all products compared to their weight in gramms of
the current day.
This can be adjusted by changing the variables below before the def main() function.
"""


CATEGORIES_TO_CHECK = None
DATE_TO_CHECK = None
STORE_TO_CHECK = None


def main():
    parser = Parser(category=CATEGORIES_TO_CHECK, store=STORE_TO_CHECK) 
    parser.start_logger()
    parser.parse_data()
    ## parser.create_graph(save_graph=True, extract_outliers=True)
    ## parser.stop_program()
    sys.exit(0)


class Parser:
    def __init__(self, date=None, category=None, store=None):
        self.data = None
        self.category = category
        self.store = store
        if date is None:
            date = datetime.now().date()
        self.date = date
        

    def start_logger(self):
        ## start process, create logger
        self.startprocess = time.process_time()
        self.start = time.time()
        logs_path = os.path.join("logs", "parser")
        os.makedirs(logs_path, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            handlers= [
                logging.FileHandler(os.path.join(logs_path, (datetime.now().strftime("%Y.%m.%d %H-%M-%S") + ".log"))),
                logging.StreamHandler()
            ],
            level=logging.ERROR,
            format="%(asctime)s: %(message)s",
            datefmt="%Y.%m.%d %H:%M:%S"
            )


    def parse_data(self):
        
        filters = []
        if self.date:
            filters.append(DailyData.date == self.date)
        if self.store:
            filters.append(DailyData.store_id == self.store)

        with SessionLocal() as session:
            #query = session.query(ProductObservations).filter(ProductObservations.date == self.date)
            query = session.query(DailyData).filter(*filters)
            data = pd.read_sql(query.statement, session.bind)
            
            if self.category:
                pass
            else:
                category_query = session.query(Categories).all()
                category_mapping = {category.category_name: category.category_id for category in category_query}
                for product in data:
                    category_check = product["category_id"]
                    if category_check in category_mapping:
                        product["category_id"] = category_mapping[category_check]

            if self.store:
                pass
            else:
                store_query = session.query(Stores).all()
                store_mapping = {store.store_name: store.store_id for store in store_query}
                for product in data:
                    store_check = product["store_id"]
                    if store_check in store_mapping:
                        product["store_id"] = store_mapping[store_check]



        data.set_index("product_id", inplace=True)
        data.loc[data["listed_unit"] == "kg", "listed_amount"] *= 1000
        data.loc[data["listed_unit"] == "l", "listed_amount"] *= 1000


        self.data = data



    def create_graph(self, dataframe, type="plotly", save_graph=False, extract_outliers=False):
        data = dataframe
        
        if extract_outliers:
            mean = (data["listed_price"].mean())
            std_dev = (data["listed_price"].std())
            data = data[(data["listed_price"] >= mean - 2 * std_dev) & (data["listed_price"] <= mean + 2 * std_dev)]
            
            mean = (data["listed_amount"].mean())
            std_dev = (data["listed_amount"].std())
            data = data[(data["listed_amount"] >= mean - 2 * std_dev) & (data["listed_price"] <= mean + 2 * std_dev)]

        if type == "plotly":
            fig = px.scatter(
                data, 
                x=data["listed_price"], 
                y=data["listed_amount"], 
                title="fruit & vegetables price distribution",
                labels={"x": "price", "y": "amount"},
                symbol=data["name"]
            )
            
            fig.show()
            if save_graph:
                fig.write_html("graph.html")
        
        
        elif type == "seaborn":
            seaborn.scatterplot(x=dataframe["listed_price"], y=dataframe["listed_amount"])
            
            plt.title("price to amount comparison")
            plt.xlabel("price")
            plt.ylabel("amount")
            
            plt.show()
            if save_graph:
                plt.savefig("graph.png")

        
        else:
            raise KeyError


    def stop_program(self):
        self.end = time.time()
        self.endprocess = time.process_time()
        self.logger.info(f"""
        FINISHED DATA ANALYSIS
        CHECK VOLUME FOR RESULTS.
        TOTAL RUNTIME: {self.start - self.end}
        TOTAL CPU RUNTIME: {self.startprocess - self.endprocess}
        """)
        sys.exit(0)


if __name__ == ("__main__"):
    main()
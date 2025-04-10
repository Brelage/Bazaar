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
from models import DailyStatistics



def main():
    parser = Parser() 
    parser.start_logger()
    parser.parse_data(extract_outliers=True)
    ## parser.create_graph(save_graph=True)
    ## parser.insert_into_database()
    ## parser.stop_program()
    sys.exit(0)


class Parser:
    def __init__(self):
        self.df = None

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


    def parse_data(self, extract_outliers=False, category=None):
        data_path = "data/20250401/20250401.csv"
        df = pd.read_csv(data_path)
        df.set_index("ID")
        
        if category:
            df = df[df["category"] == category]
        
        
        if extract_outliers:
            mean = (df["price in €"].mean())
            std_dev = (df["price in €"].std())
            df = df[(df["price in €"] >= mean - 2 * std_dev) & (df["price in €"] <= mean + 2 * std_dev)]
            
            mean = (df["€ per kg"].mean())
            std_dev = (df["€ per kg"].std())
            df = df[(df["€ per kg"] >= mean - 2 * std_dev) & (df["€ per kg"] <= mean + 2 * std_dev)]
 
        self.df = df


    def create_graph(self, dataframe, type="plotly", save_graph=False):
        df = dataframe
        
        if type == "plotly":
            fig = px.scatter(
                df, 
                x=df["price in €"], 
                y=df["€ per kg"], 
                title="fruit & vegetables price distribution",
                labels={"x": "X Axis Label", "y": "Y Axis Label"},
                symbol=df["name"]
            )
            
            fig.show()
            if save_graph:
                fig.write_html("graph.html")
        
        elif type == "seaborn":
            seaborn.scatterplot(x=dataframe["€ per kg"], y=dataframe["price in €"])
            
            plt.title("price per unit to price per KG correlation")
            plt.xlabel("price per kg")
            plt.ylabel("price per unit")
            
            plt.show()
            if save_graph:
                plt.savefig("graph.png")

        else:
            raise KeyError



    def insert_into_database(self):
        self.logger.info("connecting to database")
        db = SessionLocal()

        self.logger.info("writing to database")
        daily_stats = DailyStatistics(self.df)
        db.add(daily_stats)
        db.commit()
        db.close()


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
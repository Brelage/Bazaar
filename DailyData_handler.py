import os 
import sys
import time
import logging
import pandas as pd
from datetime import datetime
from database_engine import SessionLocal
from models import DailyStatistics, DailyData, Categories, Stores



def main():
    handler = Handler()
    handler.create_daily_statistics()
    #handler.check_availability()
    #handler.check_new_products()
    #handler.check_changes()
    #handler.empty_DailyData()
    #handler.stop_program()
    sys.exit(0)



class Handler:
    def __init__(self):
        self.daily_data = self.load_daily_data()
        self.setup_logger()
    

    def setup_logger(self):
        """
        creates a logger for monitoring both in the terminal and for reference in 
        a "logs" folder in a subfolder named after the current date.
        """
        
        logs_path = os.path.join("logs", "DailyData_handler")
        os.makedirs(logs_path, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.ERROR)  

        if not self.logger.handlers:
            formatter = logging.Formatter(fmt="%(asctime)s: %(message)s", datefmt="%Y.%m.%d %H:%M:%S")
            
            ## config for the .log file generated
            log_file = os.path.join(logs_path, f"{datetime.now().strftime('%Y.%m.%d %H-%M-%S')}.log")
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            # uncomment the line below for more granular logging config 
            # file_handler.setLevel(logging.INFO)
            self.logger.addHandler(file_handler)
            
            ## config for the stream handler that shows logs in the terminal
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            # uncomment the line below for more granular logging config 
            # stream_handler.setLevel(logging.INFO)
            self.logger.addHandler(stream_handler)


    def load_daily_data(self):
        """
        Creates a nested dictionary where the first layer are all unique dates, the second layer are all unique stores listed in 
        the DailyData table and the values are pandas dataframes containing all products with the corresponding date and store.
        This allows for distinct iteration over all datasets saved in the self.daily_data variable.
        """

        with SessionLocal() as session:
            # Get all unique dates and stores
            dates = session.query(DailyData.date).distinct().all()
            date_values = [d[0] for d in dates]
            stores = session.query(DailyData.store_id).distinct().all()
            store_values = [s[0] for s in stores]
            
            # Build nested dictionary
            daily_data = {}
            for date in date_values:
                daily_data[date] = {}
                for store in store_values:
                    query = session.query(DailyData).filter(
                        DailyData.date == date,
                        DailyData.store_id == store
                    )
                    df = pd.read_sql(query.statement, session.bind)
                    if not df.empty:
                        daily_data[date][store] = df
        
        return daily_data
        

    def create_daily_statistics(self):
        """
        calculates a set of statistical data points and
        inserts them into the DailyStatistics table in the database.
        """

        session = SessionLocal()

        for date, store_subset in self.daily_data.items():
            for store, df in store_subset.items():
                
                price_mean = (df["listed_price"].mean())
                price_median = (df["listed_price"].median())
                amount_bio_products = (df["has_bio_label"].count())
                amount_reduced_products = (df["is_on_offer"].count())
                
                daily_statistics = DailyStatistics(
                    date= date,
                    store_id = store,
                    price_mean = price_mean,
                    price_median = price_median,
                    amount_bio_products = int(amount_bio_products),
                    amount_reduced_products = int(amount_reduced_products)
                )
                
                session.add(daily_statistics)
                session.commit()



    def check_availability(self):
        """
        if product_id in Observations.is_available == True but not in dataset:
            in Observations: set is_available bool to False
        """
        pass


    def check_new_products(self):
        """
        if product_id in dataset but not in Products:
            new product row
            new ProductObservation row
            drop row from dataset
        """
        pass


    def check_changes(self):
        """
        if any relevant columns of row in dataset != row in Observations
            old row in Observations: update is_available bool to False
            new row in Observations
            drop row from dataset            
        """


    def empty_DailyData():
        with SessionLocal() as session:
            session.query(DailyData).delete()
            session.commit()


    def stop_program(self):
        pass



if __name__ == "__main__":
    main()
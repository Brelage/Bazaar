import os 
import sys
import time
import logging
import signal
import pandas as pd
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from database_engine import SessionLocal
from models import DailyStatistics, DailyData, CategoryStatistics
from sqlalchemy import update, inspect


def main():
    handler = Handler()
    signal.signal(signal.SIGTERM, Handler.shutdown)
    signal.signal(signal.SIGINT, Handler.shutdown)    
    handler.create_daily_statistics()
    #handler.check_availability()
    #handler.check_new_products()
    #handler.check_changes()
    #handler.empty_DailyData()
    handler.stop_program()



class Handler:
    def __init__(self):
        self.setup_logger()
        self.daily_data = self.load_daily_data()
    

    def setup_logger(self):
        """
        creates a logger for monitoring both in the terminal and for reference in 
        a "logs" folder in a subfolder named after the current date.
        """

        self.start = time.time()
        self.startprocess = time.process_time()
        logs_path = os.path.join("logs", "DailyData_handler")
        os.makedirs(logs_path, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)  

        if not self.logger.handlers:
            formatter = logging.Formatter(fmt="%(asctime)s: %(message)s", datefmt="%Y.%m.%d %H:%M:%S")
            
            ## config for the .log file generated
            file_handler = TimedRotatingFileHandler(
            f"logs/DailyData_handler/DailyData_handler.log",
            when="midnight",
            backupCount=30
            )
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
        Creates a nested dictionary where the first layer are all unique dates listed in the DailyData table, the second
        layer are all unique stores listed in the DailyData table and the values are pandas dataframes containing 
        all products with the corresponding date and store. 
        This allows for distinct iteration over all datasets saved in the self.daily_data variable.
        """

        self.logger.info("loading data.")
        with SessionLocal() as session:
            # Get all unique dates and stores found in the DailyData table
            date_values = [d[0] for d in (session.query(DailyData.date).distinct().all())]
            store_values = [s[0] for s in (session.query(DailyData.store_id).distinct().all())]
            
            # Build nested dictionary of datasets
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
        creates the iterative logic for calculating and inserting both the
        daily statistics and the category statistics by creating a data subset for every 
        unique combination of date and store and passing that data subset to
        the calculate_statistics function.
        """

        session = SessionLocal()

        for date, store_subset in self.daily_data.items():
            for store, df in store_subset.items():
                self.calculate_statistics(df, date, store, session)
        session.close()


    def calculate_statistics(self, df, date, store, session, category=None):
        """
        calculates a set of statistical data points and
        inserts them into the DailyStatistics table in the database.

        Args:
        df: the data subset created by the create_daily_statistics function.
        date: a date listed in the DailyData database.
        store: a store listed in the DailyData database.
        session: a session object created by the create_daily_statistics function to interact with the database.
        category: a category listed in the DailyData database. Default is set to None for calculation on the whole dataset.
        
        Output: 
        database entries both in the DailyStatistics table and in the CategoryStatistics table.
        """
        
        # this reduces the dataset to only the rows with the fitting category if a category is passed into the method
        if category != None:
            df = df[df["category_id"]== category]
            self.logger.info(f"calculating category statistics. date: {date}. category_id: {category}.")
        else:
            self.logger.info(f"calculating daily statistics. date: {date}.")


        price_min = df["listed_price"].min()
        price_max = df["listed_price"].max()
        price_mean = df["listed_price"].mean() ## average price
        price_median = df["listed_price"].median() ## middle price
        price_skewness = df["listed_price"].skew() ## asymmetry of the distribution of price points
        price_standard_deviation = df["listed_price"].std() ## how much values typically deviate from the mean
        price_variance = df["listed_price"].var() ## the square of the standard deviation
        price_range = price_max - price_min ## difference between the lowest listed price and the highest
        price_quartile_1 = df["listed_price"].quantile(0.25)  ## the price of products at the 25th percentile (cheaper than 75% of the rest)
        price_quartile_3 = df["listed_price"].quantile(0.75) ## the price of products at the 75th percentile (more expensive than 75% of the rest)
        IQR = price_quartile_3 - price_quartile_1 ## difference between the 75th and 25th percentiles

        amount_total_products = len(df)
        amount_bio_products = (df["has_bio_label"]== 1).sum()
        amount_reduced_products = (df["is_on_offer"]== 1).sum()
        percentage_reduced_products = ((amount_reduced_products / amount_total_products) * 100) if amount_reduced_products else 0
        percentage_bio_products = ((amount_bio_products / amount_total_products) * 100) if amount_bio_products else 0

        if category != None:
            green_premium = ( ## price difference between the average product with a bio label relative to the median price
                (((df.loc[df["has_bio_label"]== 1, "listed_price"]).median()) 
                 - price_median) 
                 if amount_bio_products else 0)
        
        if category != None:
            average_savings = ( ## price difference between the average product with a reduced price relative to the median price
                (((df.loc[df["is_on_offer"]== 0, "listed_price"]).median()) 
                - price_median) 
                if amount_reduced_products else 0)


        if category == None:
            daily_statistics = DailyStatistics(
                date= date,
                store_id = store,

                price_min = round(price_min, 4),
                price_max = round(price_max, 4),
                price_mean = round(price_mean, 4),
                price_median = round(price_median, 4),
                price_skewness = round(price_skewness, 3),
                price_standard_deviation = round(price_standard_deviation, 4),
                price_variance = round(price_variance, 4),
                price_range = round(price_range, 4),
                price_quartile_1 = round(price_quartile_1, 4),
                price_quartile_3 = round(price_quartile_3, 4),
                IQR = round(IQR, 4),

                amount_total_products = int(amount_total_products),
                amount_bio_products = int(amount_bio_products),
                amount_reduced_products = int(amount_reduced_products),
                percentage_bio_products = round(percentage_bio_products, 4),
                percentage_reduced_products = round(percentage_reduced_products, 4),
            )
            
            self.logger.info("inserting daily statistics into database.\n")
            self.upsert(session=session, instance=daily_statistics)
        
        else:
            category_statistics = CategoryStatistics(
                date= date,
                store_id = store,
                category_id= category,

                price_min = round(price_min, 4),
                price_max = round(price_max, 4),
                price_mean = round(price_mean, 4),
                price_median = round(price_median, 4),
                price_skewness = round(price_skewness, 3),
                price_standard_deviation = round(price_standard_deviation, 4),
                price_variance = round(price_variance, 4),
                price_range = round(price_range, 4),
                price_quartile_1 = round(price_quartile_1, 4),
                price_quartile_3 = round(price_quartile_3, 4),
                IQR = round(IQR, 4),

                amount_total_products = int(amount_total_products),
                amount_bio_products = int(amount_bio_products),
                amount_reduced_products = int(amount_reduced_products),
                percentage_bio_products = round(percentage_bio_products, 4),
                percentage_reduced_products = round(percentage_reduced_products, 4),

                green_premium = round(green_premium, 4),
                average_savings = round(average_savings, 4)
            )

            self.logger.info("inserting category statistics into database.\n")
            self.upsert(session=session, instance=category_statistics)

        # this part extracts all categories listed in the dataset and recursively creates statistics for each category
        # once the daily statistics and all category statistics are calculated and inserted, the next dataset is iterated upon
        if category == None:
            category_datapoints = df["category_id"].unique().tolist()
            category_datapoints.sort()
            for category_key in category_datapoints:
                self.calculate_statistics(df, date, store, session, category=category_key)


    def upsert(self, session, instance):
        """
        upsert multiple rows into a given table with a composite primary key using a Session.

        Args:
        session: SQLAlchemy Session object
        instance: SQLAlchemy ORM instance
        """
        model = type(instance)
        primary_keys = [key.name for key in inspect(model).primary_key]
        row = {c.name: getattr(instance, c.name) for c in model.__table__.columns}
        key_fields = {key: row[key] for key in primary_keys}
        update_fields = {key: value for key, value in row.items() if key not in primary_keys}

        statement = (
            update(model)
            .where(*(getattr(model, key) == value for key, value in key_fields.items()))
            .values(**update_fields)
        )
        result = session.execute(statement)
        
        if result.rowcount == 0:
            session.add(instance)
        
        session.commit()


    def check_availability(self):
        """
        if product_id in Observations.is_available == True but not in dataset:
            in Observations: set is_available bool to False
        """

        self.logger.info("comparing availability with existing products.")
        pass


    def check_new_products(self):
        """
        if product_id in dataset but not in Products:
            new product row
            new ProductObservation row
            drop row from dataset
        """

        self.logger.info("checking for new products in dataset.")
        pass


    def check_changes(self):
        """
        if any relevant columns of row in dataset != row in Observations
            old row in Observations: update is_available bool to False
            new row in Observations
            drop row from dataset            
        """

        self.logger.info("checking for changes between products in dataset and ProductObservations.")
        pass


    def empty_DailyData(self):
        """
        deletes all rows from the DailyData table after dispersing relevant data to the other tables. 
        """

        self.logger.info("removing dataset from DailyData table.")
        with SessionLocal() as session:
            session.query(DailyData).delete()
            session.commit()


    def stop_program(self, success=True):
        """
        logs total runtime and total CPU runtime. 
        Exits the program.
        """

        self.logger.info("stopping program.")
        self.end = time.time()
        self.endprocess = time.process_time()
        if success:
            self.logger.info(f"""
                \nFINISHED DISPERSING DATA FROM DAILYDATA TABLE.
                \nCHECK VOLUME FOR SCRAPED DATA.
                \nTOTAL RUNTIME: {int((self.end - self.start) // 60)} minutes and {int((self.end - self.start) % 60)} seconds (precice: {round(self.end - self.start, 4)} seconds)
                \nTOTAL CPU RUNTIME: {round(self.endprocess - self.startprocess, 2)} seconds
                """)
        else: 
            self.logger.error(f"""
                \nDISPERSING DATA FROM DAILYDATA TABLE UNSUCCESSFUL.
                \nCHECK LOGS FOR ERROR CODES.
                \nTOTAL RUNTIME: {int((self.end - self.start) // 60)} minutes and {int((self.end - self.start) % 60)} seconds (precice: {round(self.end - self.start, 4)} seconds)
                \nTOTAL CPU RUNTIME: {round(self.endprocess - self.startprocess, 2)} seconds
                """)
        sys.exit(0)


    def shutdown(self, signum, frame):
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop_program(success=False)


if __name__ == "__main__":
    main()
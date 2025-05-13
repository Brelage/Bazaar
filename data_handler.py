import logging
import os
import signal
import sys
import time

from logging.handlers import TimedRotatingFileHandler

import pandas as pd
from sqlalchemy import select, update, func

import db_utils
import models
from config import LOG_LEVEL


def main():
    handler = Handler()
    signal.signal(signal.SIGTERM, Handler.shutdown)
    signal.signal(signal.SIGINT, Handler.shutdown)
    handler.create_daily_statistics()
    #handler.check_new_products()
    #handler.check_availability()
    #handler.check_changes()
    #handler.empty_DailyData()
    handler.stop_program()



class Handler:
    def __init__(self):
        self.setup_logger()
        self.daily_data = self.load_daily_data()
    

    def setup_logger(self):
        """
        creates a timed rotating logger for monitoring both in 
        the terminal and for reference in a "logs" folder.
        """

        self.start = time.time()
        self.startprocess = time.process_time()
        logs_path = os.path.join("logs", "data_handler")
        os.makedirs(logs_path, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(LOG_LEVEL)  

        if not self.logger.handlers:
            formatter = logging.Formatter(fmt="%(asctime)s: %(message)s", datefmt="%Y.%m.%d %H:%M:%S")
            
            ## config for the .log file generated
            file_handler = TimedRotatingFileHandler(
            f"logs/data_handler/data_handler.log",
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
        # Get all unique dates and stores found in the DailyData table
        with db_utils.session_query() as session:
            date_values = [d[0] for d in (session.query(models.DailyData.date).distinct().all())]
            store_values = [s[0] for s in (session.query(models.DailyData.store_id).distinct().all())]
                
            # Build nested dictionary of datasets
            daily_data = {}
            for date in date_values:
                daily_data[date] = {}
                for store in store_values:
                    query = session.query(models.DailyData).filter(
                        models.DailyData.date == date,
                        models.DailyData.store_id == store
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

        for date, store_subset in self.daily_data.items():
            for store, df in store_subset.items():
                self.calculate_statistics(df, date, store)


    def calculate_statistics(self, df, date, store, category=None):
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
        price_mean = df["listed_price"].mean()
        price_median = df["listed_price"].median()
        price_skewness = df["listed_price"].skew()
        price_standard_deviation = df["listed_price"].std()
        price_variance = df["listed_price"].var()
        price_range = price_max - price_min
        price_quartile_1 = df["listed_price"].quantile(0.25)
        price_quartile_3 = df["listed_price"].quantile(0.75)
        IQR = price_quartile_3 - price_quartile_1 

        amount_total_products = len(df)
        amount_bio_products = (df["has_bio_label"]== 1).sum()
        amount_reduced_products = (df["is_on_offer"]== 1).sum()
        percentage_reduced_products = ((amount_reduced_products / amount_total_products) * 100) if amount_reduced_products else 0
        percentage_bio_products = ((amount_bio_products / amount_total_products) * 100) if amount_bio_products else 0

        if category != None:
            green_premium = (
                (((df.loc[df["has_bio_label"]== 1, "listed_price"]).median()) 
                 - price_median) 
                 if amount_bio_products else 0)
        
        if category != None:
            average_savings = (
                (((df.loc[df["is_on_offer"]== 0, "listed_price"]).median()) 
                - price_median) 
                if amount_reduced_products else 0)


        if category == None:
            daily_statistics = {
                "date": date,
                "store_id": store,
                "price_min": round(price_min, 4),
                "price_max": round(price_max, 4),
                "price_mean": round(price_mean, 4),
                "price_median": round(price_median, 4),
                "price_skewness": round(price_skewness, 3),
                "price_standard_deviation": round(price_standard_deviation, 4),
                "price_variance": round(price_variance, 4),
                "price_range": round(price_range, 4),
                "price_quartile_1": round(price_quartile_1, 4),
                "price_quartile_3": round(price_quartile_3, 4),
                "IQR": round(IQR, 4),
                "amount_total_products": int(amount_total_products),
                "amount_bio_products": int(amount_bio_products),
                "amount_reduced_products": int(amount_reduced_products),
                "percentage_bio_products": round(percentage_bio_products, 4),
                "percentage_reduced_products": round(percentage_reduced_products, 4),
            }
            
            self.logger.info("inserting daily statistics into database.\n")
            db_utils.bulk_upsert(models.DailyStatistics, daily_statistics)
        
        else:
            category_statistics = {
                "date": date,
                "store_id": store,
                "category_id": category,
                "price_min": round(price_min, 4),
                "price_max": round(price_max, 4),
                "price_mean": round(price_mean, 4),
                "price_median": round(price_median, 4),
                "price_skewness": round(price_skewness, 3),
                "price_standard_deviation": round(price_standard_deviation, 4),
                "price_variance": round(price_variance, 4),
                "price_range": round(price_range, 4),
                "price_quartile_1": round(price_quartile_1, 4),
                "price_quartile_3": round(price_quartile_3, 4),
                "IQR": round(IQR, 4),
                "amount_total_products": int(amount_total_products),
                "amount_bio_products": int(amount_bio_products),
                "amount_reduced_products": int(amount_reduced_products),
                "percentage_bio_products": round(percentage_bio_products, 4),
                "percentage_reduced_products": round(percentage_reduced_products, 4),
                "green_premium": round(green_premium, 4),
                "average_savings": round(average_savings, 4)
            }

            self.logger.info("inserting category statistics into database.\n")
            db_utils.bulk_upsert(models.CategoryStatistics, category_statistics)

        # this part extracts all categories listed in the dataset and recursively creates statistics for each category
        # once the daily statistics and all category statistics are calculated and inserted, the next dataset is iterated upon
        if category == None:
            category_datapoints = df["category_id"].unique().tolist()
            category_datapoints.sort()
            for category_key in category_datapoints:
                self.calculate_statistics(df, date, store, category=category_key)


    def check_new_products(self):
        """
        checks if there are products in DailyData but not yet in Products.
        Creates an entry in both the products table and in the product_observations table.
        """

        self.logger.info("setting up data to check for new products.")
        
        ## gets all product IDs already in the Products table 
        with db_utils.session_query() as session:
            product_ids = [p for (p,) in session.query(models.Products.product_id).distinct().all()]
        
        ## flattens the DailyData dataset into a single data frame 
        new_products_grouped = []
        for date in self.daily_data:
            for store in self.daily_data[date]:
                new_products_grouped.append(self.daily_data[date][store])
        new_products = pd.concat(new_products_grouped)

        existing_products = []
        for index, value in new_products["product_id"].items():
            if value in product_ids:
                existing_products.append(index)

        ## updates the Products table with all products that are new
        new_products_table = (
            new_products
            .drop(columns=["date", 
                           "store_id",  
                           "listed_price", 
                           "listed_amount", 
                           "listed_unit", 
                           "is_on_offer"], axis=1)
            .drop(existing_products)
            .drop_duplicates(subset=["product_id"])
            .to_dict(orient="records")
        )
        if new_products_table:
            self.logger.info("updating database with new products.")
            db_utils.bulk_upsert(models.Products, new_products_table)

        ## updates the ProductObservations table with all products that are new
        with db_utils.session_query() as session:
            existing_observations = set(
                session.query(
                    models.ProductObservations.product_id,
                    models.ProductObservations.store_id
                ).distinct().all()
            )

        latest_entries = (
            new_products
            .sort_values("date")
            .groupby(["product_id", "store_id"], as_index=False)
            .last()
        )
        
        def pair_exists(row):
            return (row["product_id"], row["store_id"]) in existing_observations
        
        new_observations_df = latest_entries[~latest_entries.apply(pair_exists, axis=1)]

        new_observations_df["is_available"] = True
        new_observations = (
            new_observations_df
            .drop(columns=["product_name", 
                           "has_bio_label",
                           "category_id"], axis=1)
            .to_dict(orient="records")
        )

        if new_observations:
            self.logger.info("updating database with product observations.")
            with db_utils.session_commit() as session:
                session.bulk_insert_mappings(models.ProductObservations, new_observations)


    def check_availability(self):
        """
        if product_id in Observations.is_available == True but not in dataset:
            in Observations: set is_available bool to False
        """

        self.logger.info("comparing availability with existing products.")
        
        latest_date_subq = (
            select(
                models.DailyData.product_id,
                func.max(models.DailyData.date).label("max_date")
            )
            .group_by(models.DailyData.product_id)
            .subquery()
        )

        latest_product_ids_subq = (
            select(models.DailyData.product_id)
            .join(
                latest_date_subq,
                (models.DailyData.product_id == latest_date_subq.c.product_id) &
                (models.DailyData.date == latest_date_subq.c.max_date)
            ).subquery()
        )
        set_unavailable = (
            update(models.ProductObservations)
            .where(~models.ProductObservations.product_id.in_(select(latest_product_ids_subq)))
            .values(is_available=False)
        )

        with db_utils.session_commit() as session:
            session.execute(set_unavailable)


    def check_changes(self):
        """
        if any relevant columns of row in dataset != row in Observations
            old row in Observations: update is_available bool to False
            new row in Observations
            drop row from dataset
        """

        self.logger.info("checking for changes between products in dataset and ProductObservations.")
        
        new_products_grouped = []
        for date in self.daily_data:
            for store in self.daily_data[date]:
                new_products_grouped.append(self.daily_data[date][store])
        new_products = (pd
                        .concat(new_products_grouped)
                        .sort_values("date")
                        .groupby(["product_id", "store_id"], as_index=False)
                        .last()
                        .drop(columns=["product_name", "has_bio_label", "category_id"], axis=1)
            )

        with db_utils.session_query() as session:
            query = session.query(models.ProductObservations).where(
                models.ProductObservations.is_available == True
            )
            latest_observations = pd.read_sql(query.statement, session.bind)

        # Merge on primary keys
        primary_keys = ["store_id", "product_id"]
        comparison_columns = ["listed_price", "listed_amount", "listed_unit", "is_on_offer"]
        merged = pd.merge(
            new_products,
            latest_observations,
            on=primary_keys,
            how="left",
            suffixes=("", "_obs")
        )

        # Find rows where any comparison column differs
        def row_changed(row):
            for col in comparison_columns:
                if row[f"{col}"] != row[f"{col}_obs"]:
                    return True
            return False

        changed_mask = merged.apply(row_changed, axis=1)
        changed_products = merged[changed_mask]

        # Insert new observations
        insert_columns = primary_keys + ["date"] + [f"{col}" for col in comparison_columns]
        to_insert = changed_products[insert_columns].copy()
        to_insert.columns = primary_keys + ["date"] + comparison_columns
        to_insert["is_available"] = True
        records = (to_insert
                   .drop_duplicates(subset=comparison_columns)
                   .to_dict(orient="records"))

        if records:
            self.logger.info("Inserting changed product observations.")
            with db_utils.session_commit() as session:
                session.bulk_insert_mappings(models.ProductObservations, records)

            # Mark previous as unavailable
            with db_utils.session_commit() as session:
                for _, row in changed_products.iterrows():
                    session.query(models.ProductObservations).filter(
                        models.ProductObservations.product_id == row["product_id"],
                        models.ProductObservations.store_id == row["store_id"],
                        models.ProductObservations.is_available == True
                    ).update({"is_available": False})


    def empty_DailyData(self):
        """
        deletes all rows from the DailyData table after dispersing relevant data to the other tables. 
        """

        self.logger.info("removing dataset from DailyData table.")
        with db_utils.session_commit() as session:
            session.query(models.DailyData).delete()


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
import os
import sys
from src.components.data_ingestion import DataIngestion
from src.exception import CustomException
from src.logger import logging
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from dataclasses import dataclass


@dataclass
class DataCleaningConfig:
    raw_data_path: str = os.path.join('data', 'raw', "gs_projects.csv")
    cleaned_data_path: str = os.path.join('data', 'staging', "data.csv")
    train_data_path: str = os.path.join('data', 'staging', "train.csv")
    test_data_path: str = os.path.join('data', 'staging', "test.csv")


class DataCleaning:
    def __init__(self):
        self.cleaning_config = DataCleaningConfig()

    def initiate_data_cleaning(self):
        logging.info("Entered the data cleaning method or component")
        try:

            col_datatypes = self.get_data_types()
            df = pd.read_csv(
                self.cleaning_config.raw_data_path, dtype=col_datatypes)

            # Parsing date-time columns
            df['created_at'] = pd.to_datetime(df['created_at'])
            df['updated_at'] = pd.to_datetime(df['updated_at'])
            df['crediting_period_start_date'] = pd.to_datetime(df['crediting_period_start_date'])
            df['crediting_period_end_date'] = pd.to_datetime(df['crediting_period_end_date'])

            # Adding False for Goal columns NaN values
            goal_cols = df.filter(like='Goal_').columns
            df[goal_cols] = goal_cols.fillna(False)

            # Filling NaN with zero for credits
            df.VER_issued_credits.fillna(0, inplace=True)
            df.VER_retired_credits.fillna(0, inplace=True)

            # Replacing typos
            replace_dict = {'Micro scale': 'Micro Scale',
                            'Microscale': 'Micro Scale', 'Large scale': 'Large Scale'}
            df = df.replace({'size': replace_dict})

            df.to_csv(self.cleaning_config.cleaned_data_path)

            logging.info("Data cleaning is completed")
            print("Data cleaning is completed")

        except Exception as e:
            raise CustomException(e, sys)

    def get_data_types(self):

        return {'id': 'Int64', 'estimated_annual_credits': 'Int64', 'poa_project_id': 'Int64',
                'poa_project_sustaincert_id': 'Int64', 'latitude': 'float', 'longitude': 'float',
                'VER_retired_credits': 'float', 'VER_issued_credits': 'float'}


if __name__ == "__main__":
    #data_injestion = DataIngestion()
    #data_injestion.initiate_data_ingestion()

    data_cleaning = DataCleaning()
    data_cleaning.initiate_data_cleaning()

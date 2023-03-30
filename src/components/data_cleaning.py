import os
import sys
from src.components.data_ingestion import DataIngestion
from src import utils
from src.exception import CustomException
from src.logger import logging
import pandas as pd

from dataclasses import dataclass


@dataclass
class DataCleaningConfig:
    raw_data_path: str = os.path.join(
        'data', 'raw', "gs_certified_projects.csv")
    cleaned_data_path: str = os.path.join('data', 'staging', "data.csv")

class DataCleaning:
    def __init__(self):
        self.cleaning_config = DataCleaningConfig()

    def initiate_data_cleaning(self):
        logging.info("Entered the data cleaning method or component")

        try:
            df = utils.read_file_with_custom_data_types(
                self.cleaning_config.raw_data_path)

            # Adding False for Goal columns NaN values
            goal_cols = df.filter(like='Goal_').columns
            df[goal_cols] = df[goal_cols].fillna(False)

            # Filling NaN with zero for credits
            df.VER_issued_credits.fillna(0, inplace=True)
            df.VER_retired_credits.fillna(0, inplace=True)

            # Dropping columns
            df.drop('status', axis=1, inplace=True)
            df.drop('state', axis=1, inplace=True)

            # Replacing categorical typos
            replace_dict = {'Micro scale': 'Micro Scale',
                            'Microscale': 'Micro Scale', 'Large scale': 'Large Scale'}
            df = df.replace({'size': replace_dict})

            # Replacing Unknown or inter-national waters country codes
            # Name changed from East Timor (TP, TMP, 626) to Timor-Leste (TL, TLS, 626)
            # XZ -> Inter-national waters
            country_dict = {'XZ': 'US', 'TL': 'TP'}
            df = df.replace({'country_code': country_dict})

            # Handling case: If retired credits are greater than the issued ones.
            df.loc[df['VER_retired_credits'] > df['VER_issued_credits'],
                   'VER_retired_credits'] = df['VER_issued_credits']

            df.to_csv(self.cleaning_config.cleaned_data_path,
                      index=False, header=True)

            logging.info("Data cleaning is completed")
            print("Data cleaning is completed")

        except Exception as e:
            raise CustomException(e, sys)

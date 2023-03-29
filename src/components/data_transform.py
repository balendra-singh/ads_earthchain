import os
import sys
from src import utils
from src.components.data_cleaning import DataCleaning
from src.components.data_ingestion import DataIngestion
from src.exception import CustomException
from src.logger import logging
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from dataclasses import dataclass


@dataclass
class DataTransformConfig:
    cleaned_data_path: str = os.path.join('data', 'staging', "data.csv")
    transform_data_path: str = os.path.join('data', 'transformed', "transform.csv")
    train_data_path: str = os.path.join('data', 'staging', "train.csv")
    test_data_path: str = os.path.join('data', 'staging', "test.csv")


class DataTransform:
    def __init__(self):
        self.transform_config = DataTransformConfig()

    def initiate_data_transform(self):
        try:
            df = utils.read_file_with_custom_data_types(
                self.transform_config.cleaned_data_path)

            df = self.add_new_features(df)

            df.to_csv(self.transform_config.transform_data_path)

        except Exception as e:
            raise CustomException(e, sys)

    def add_new_features(self, project_df):

        project_df['crediting_days'] = (
            project_df['crediting_period_end_date'] - project_df['crediting_period_start_date']).dt.days
        project_df['VER_sold_percentage'] = (
            project_df['VER_retired_credits']/project_df['VER_issued_credits']) * 100
        project_df['VER_sold_percentage_per_day'] = (
            project_df['VER_sold_percentage']/project_df['crediting_days']) * 100

        return project_df


if __name__ == "__main__":

    # data_injestion = DataIngestion()
    # data_injestion.initiate_data_ingestion()

    # data_cleaning = DataCleaning()
    # data_cleaning.initiate_data_cleaning()

    data_transform = DataTransform()
    data_transform.initiate_data_transform()

import os
import sys
from src import utils

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder,StandardScaler

from src.components.data_cleaning import DataCleaning
from src.components.data_ingestion import DataIngestion
from src.exception import CustomException
from src.logger import logging
import pandas as pd
import numpy as np
import pycountry_convert as pycountry

from dataclasses import dataclass


@dataclass
class DataTransformationConfig:
    cleaned_data_path: str = os.path.join('data', 'staging', "data.csv")
    transform_data_path: str = os.path.join(
        'data', 'transformed', "transform.csv")

class DataTransformation:
    def __init__(self):
        self.transform_config = DataTransformationConfig()

    def initiate_data_transform(self):
        try:
            df = utils.read_file_with_custom_data_types(
                self.transform_config.cleaned_data_path)

            df = self.add_new_features(df)

            df.to_csv(self.transform_config.transform_data_path,
                      index=False, header=True)

            preprocessing_obj= self.get_data_transformer_object()

            #encoded_df = preprocessing_obj.fit_transform(df)
            

            
            
            logging.info("Data transformation is completed")
            print("Data transformation is completed")
            
        except Exception as e:
            raise CustomException(e, sys)

    def add_new_features(self, project_df):

        project_df['crediting_days'] = (
            project_df['crediting_period_end_date'] - project_df['crediting_period_start_date']).dt.days

        project_df['VER_sold_percentage'] = project_df.apply(lambda x: self.get_percentage(
            x['VER_retired_credits'], x['VER_issued_credits']), axis=1)

        project_df['VER_sold_percentage_per_day'] = project_df.apply(
            lambda x: self.get_percentage(x['VER_sold_percentage'], x['crediting_days']), axis=1)

        project_df['continent_code'] = project_df['country_code'].apply(
            pycountry.country_alpha2_to_continent_code)

        return project_df

    def get_percentage(self, numerator, denominator):
        if (numerator == 0 or denominator == 0):
            return 0

        return (numerator/denominator) * 100
    
    def get_data_transformer_object(self):
         try:
                numerical_columns = ["VER_sold_percentage_per_day"]
                categorical_columns = [
                    "continent_code",
                    "size",
                    "type",
                ]

                num_pipeline= Pipeline(
                    steps=[
                    # ("imputer",SimpleImputer(strategy="median")),
                    ("scaler",StandardScaler())
                    ]
                )

                cat_pipeline=Pipeline(
                    steps=[
                    # ("imputer",SimpleImputer(strategy="most_frequent")),
                    ("one_hot_encoder",OneHotEncoder()),
                    ("scaler",StandardScaler(with_mean=False))
                    ]
                )

                logging.info(f"Categorical columns: {categorical_columns}")
                logging.info(f"Numerical columns: {numerical_columns}")

                preprocessor=ColumnTransformer(
                    [
                    ("num_pipeline",num_pipeline,numerical_columns),
                    ("cat_pipelines",cat_pipeline,categorical_columns)
                    ],
                )

                preprocessor.set_output(transform="pandas")

                return preprocessor
            
         except Exception as e:
            raise CustomException(e,sys)

if __name__ == "__main__":

    #data_injestion = DataIngestion()
    #data_injestion.initiate_data_ingestion()

    data_cleaning = DataCleaning()
    data_cleaning.initiate_data_cleaning()

    data_transform = DataTransformation()
    data_transform.initiate_data_transform()

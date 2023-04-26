import os
import sys
from src import utils

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.components.data_cleaning import DataCleaning
from src.components.data_ingestion import DataIngestion
from src.exception import CustomException
from src.logger import logging
import pandas as pd
import numpy as np
import pycountry_convert as pycountry

from dataclasses import dataclass
from datetime import datetime


@dataclass
class DataTransformationConfig:
    cleaned_data_path: str = os.path.join('data', 'staging', "data.csv")
    transform_data_path: str = os.path.join(
        'data', 'transformed', "transform.csv")
    preprocessing_obj_path: str = os.path.join(
        'artifacts', "preprocessor.pkl")


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

            preprocessing_obj = self.get_data_transformer_object()

            utils.save_object(
                self.transform_config.preprocessing_obj_path, preprocessing_obj)

            logging.info("Data transformation is completed")
            print("Data transformation is completed")

        except Exception as e:
            raise CustomException(e, sys)

    def add_new_features(self, project_df):

        # Using today's date as the end of crediting period
        project_df.loc[pd.to_datetime(project_df['crediting_period_end_date'])
                       > datetime.today(), 'crediting_days'] = (datetime.today() - pd.to_datetime(project_df[pd.to_datetime(project_df['crediting_period_end_date']) > datetime.today()]['crediting_period_start_date'])).dt.days

        project_df['VER_sold_percentage'] = project_df.apply(lambda x: self.get_percentage(
            x['VER_retired_credits'], x['VER_issued_credits']), axis=1)

        project_df['VER_sold_percentage_per_day'] = project_df.apply(
            lambda x: self.get_percentage(x['VER_sold_percentage'], x['crediting_days']), axis=1)

        project_df['continent_code'] = project_df['country_code'].apply(
            self.country_to_continent)

        project_df['sector'] = project_df['type'].apply(
            self.project_sector_split)
        project_df['type'] = project_df['type'].apply(self.project_type_split)

        return project_df

    def get_percentage(self, numerator, denominator):
        if (numerator == 0 or denominator == 0):
            return 0

        return (numerator/denominator) * 100

    def country_to_continent(self, country_alpha2):
        country_continent_code = pycountry.country_alpha2_to_continent_code(
            country_alpha2)
        country_continent_name = pycountry.convert_continent_code_to_continent_name(
            country_continent_code)
        return country_continent_name

    def project_type_split(self, project_type):
        Type = project_type.split(' - ')
        if Type[0] == 'Small, Low':
            Type[0] = Type[1]
        return (Type[0])

    def project_sector_split(self, project_type):
        Type = project_type.split(' - ')
        if len(Type) < 2:
            return project_type
        if Type[0] == 'Small, Low':
            Type[1] = 'Electricity'
        return (Type[1])

    def get_data_transformer_object(self):
        try:
            numerical_columns = ["VER_sold_percentage_per_day"]
            categorical_columns = [
                "continent_code",
                "size",
                "type",
            ]

            num_pipeline = Pipeline(
                steps=[
                    # ("imputer",SimpleImputer(strategy="median")),
                    ("scaler", StandardScaler())
                ]
            )

            cat_pipeline = Pipeline(
                steps=[
                    # ("imputer",SimpleImputer(strategy="most_frequent")),
                    ("one_hot_encoder", OneHotEncoder()),
                    ("scaler", StandardScaler(with_mean=False))
                ]
            )

            logging.info(f"Categorical columns: {categorical_columns}")
            logging.info(f"Numerical columns: {numerical_columns}")

            preprocessor = ColumnTransformer(
                [
                    ("num_pipeline", num_pipeline, numerical_columns),
                    ("cat_pipelines", cat_pipeline, categorical_columns)
                ],
            )

            preprocessor.set_output(transform="pandas")

            return preprocessor

        except Exception as e:
            raise CustomException(e, sys)


if __name__ == "__main__":

    # data_injestion = DataIngestion()
    # data_injestion.initiate_data_ingestion()

    data_cleaning = DataCleaning()
    data_cleaning.initiate_data_cleaning()

    data_transform = DataTransformation()
    data_transform.initiate_data_transform()

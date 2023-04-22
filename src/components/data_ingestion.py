import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd

from dataclasses import dataclass
from src.components.gs_api import GSAPI


@dataclass
class DataIngestionConfig:
    raw_data_path: str = os.path.join(
        'data', 'raw', "gs_certified_projects.csv")


class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        logging.info("Entered the data ingestion method or component")
        try:

            api_obj = GSAPI()
            gs_project_data = api_obj.scrape_gold_standard_api(
                gs_certified_projects=True)
            df = pd.DataFrame(gs_project_data)

            df = api_obj.scrape_credit_api(df)
            df = self.get_project_goals(df)

            logging.info('Read the dataset as dataframe')

            os.makedirs(os.path.dirname(
                self.ingestion_config.raw_data_path), exist_ok=True)

            df.to_csv(self.ingestion_config.raw_data_path,
                      index=False, header=True)

            logging.info("Ingestion of the data is completed")
            print("Ingestion of the data is completed")

        except Exception as e:
            raise CustomException(e, sys)

    def get_project_goals(self, project_df):

        for index, row in project_df.iterrows():
            details_df = pd.json_normalize(
                row["sustainable_development_goals"])

            for i, r1 in details_df.iterrows():
                col_name = r1['name'].split(':')[0].replace(' ', '_')
                project_df.at[index, col_name] = int(1)

        return project_df

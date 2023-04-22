import http.client
import json
import os
import sys
from src.exception import CustomException
from src.logger import logging

from dataclasses import dataclass


@dataclass
class GSAPIConfig:
    api_hostname: str = "public-api.goldstandard.org"


class GSAPI:
    def __init__(self):
        self.api_config = GSAPIConfig()
        self.conn = http.client.HTTPSConnection(
            self.api_config.api_hostname)
        self.payload = ''
        self.headers = {'Content-Type': 'application/json'}

    def scrape_gold_standard_api(self, gs_certified_projects=False):
        try:
            gs_certified_project_list = []
            page_counter = 1

            while (True):

                if (gs_certified_projects):
                    self.conn.request("GET", f"/projects?size=150&page={page_counter}&gsCertified=true", self.payload,
                                      self.headers)
                else:
                    self.conn.request("GET", f"/projects?size=150&page={page_counter}", self.payload,
                                      self.headers)

                res = self.conn.getresponse()
                response_json = json.loads(res.read().decode())

                if (len(response_json) == 0):
                    logging.info(
                        f"Data retrieved till page number {page_counter}")
                    break

                gs_certified_project_list += response_json
                page_counter += 1

            logging.info(
                f"Total Gold Standard certified projects found are {len(gs_certified_project_list)}")
            print(
                f"Total Gold Standard certified projects found are {len(gs_certified_project_list)}")

            return gs_certified_project_list

        except Exception as e:
            raise CustomException(e, sys)

    def scrape_credit_api(self, project_df):
        try:

            for index, row in project_df.iterrows():
                self.conn.request(
                    "GET", f'/projects/{row["id"]}/credits/summary', self.payload, self.headers)
                credits = json.loads(self.conn.getresponse().read().decode())

                for i in range(len(credits)):
                    for j in range(len(credits[i]['summary'])):
                        if (credits[i]['product'] == 'VER'):
                            project_df.at[index, credits[i]['product'] + '_' + credits[i]['summary']
                                          [j]['status'].lower() + '_credits'] = credits[i]['summary'][j]['total']

            logging.info(
                f"Credits data for Gold Standard certified projects has been scraped from API")

            return project_df

        except Exception as e:
            raise CustomException(e, sys)

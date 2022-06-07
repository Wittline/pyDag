from google.cloud import bigquery
from engines.bq.bqerror import BQError
import configparser
import os

class BQClient:
    def __init__(self, logger):
        self.logger = logger

    def __get_client_service_account(self):
        config = configparser.ConfigParser()
        config.read_file(open(os.getcwd() + '/app/config/config.cfg'))
        return bigquery.Client.from_service_account_json(
            config.get('GCP','service-account'))

    def run_script(self, script, params):
        client = self.__get_client_service_account()
        script = script.format(**params)
        query_job = client.query(script)
        if query_job.errors:
            self.logger.info(9,[query_job.errors], True, BQError)            

        return True
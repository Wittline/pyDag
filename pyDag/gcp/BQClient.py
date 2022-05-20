from google.cloud import bigquery
from gcp.bqerror import BQError
import configparser

class BQClient:
    def __init__(self):
        pass

    def __get_client_service_account(self):
        config = configparser.ConfigParser()
        config.read_file(open('config/config.cfg'))
        return bigquery.Client.from_service_account_json(
            config.get('GCP','service-account'))

    def run_script(self, script, params):
        client = self.__get_client_service_account()
        query_job = client.query(script.format(**params))
        if query_job.errors:
            raise BQError('Error processing query: {0}'.format(query_job.errors))

        return True

import sys
import json
from google.oauth2 import service_account
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import configparser
from spark.dperror import DPError
import argparse
import re


class DPClient:

    def __init__(self):
        self.project_id = None
        self.region = None
        self.cluster_name = None
        self.bucket = None
        self.script_name = None
        self.__credentials = None
    

    def __get_gredentials(self):

        if self.__credentials is not None:
            return self.__credentials
        else:
            config = configparser.ConfigParser()
            config.read_file(open('config/config.cfg'))
            json_account = json.loads(config.get('GCP','service-account'))
            self.__credentials = service_account.Credentials.from_service_account_info(
                json_account)
            
        return self.__credentials

    
    def __get_client(self, client):

        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(self.region)}
        credentials = self.__get_gredentials()
        client = None

        if client == 'cluster':
            client = dataproc.ClusterControllerClient(
                client_options=client_options,
                credentials = credentials
            )
        elif client == 'job':
            client = dataproc.JobControllerClient(
                client_options = client_options,
                credentials = credentials
            )
        else:
            pass

        return client        


    def __create_cluster(self):


        client = self.__get_client('cluster')

      
        cluster = {
            "project_id": self.project_id,
            "cluster_name": self.cluster_name,
            "config": {
                "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
                "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
            },
        }


        operation = client.create_cluster(
            request={"project_id": self.project_id, "region": self.region, "cluster": cluster}
        )

        result = operation.result()
      
        print(f"Cluster created successfully: {result.cluster_name}")

                                    
    def __delete_cluster(self):

        client = self.__get_client('cluster')

        operation = client.delete_cluster(
            request={
                "project_id": self.project_id,
                "region": self.region,
                "cluster_name": self.cluster_name,
            }
        )
        operation.result()

        print("Cluster {} successfully deleted.".format(self.cluster_name))


    def __submit_job(self):

        client = self.__get_client('job')
        
        job = {
            "placement": {"cluster_name": self.cluster_name},          
            "pyspark_job": {"main_python_file_uri": "gs://{}/{}".format(self.bucket, self.script_name)},
        }

        operation = client.submit_job_as_operation(
            request={"project_id": self.project_id, "region": self.region, "job": job}
        )

        response = operation.result()

        matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

        output = (
            storage.Client()
            .get_bucket(matches.group(1))
            .blob(f"{matches.group(2)}.000000000")
            .download_as_string()
        )

        print(f"Job finished successfully: {output}")


    def run_script(self, script):

        client = self.__get_client_service_account()
        query_job = client.query(script)
        if script_job.errors:
            raise DPError('Error processing pyspark script: {0}'.format(query_job.errors))

        return True    
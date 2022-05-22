import sys
import json
from google.oauth2 import service_account
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import configparser
from spark.dperror import DPError
import argparse
import re
import time


class DPClient:

    def __init__(self):
        self.project_id = None
        self.region = None
        self.cluster_name = None
        self.bucket = None
        self.folder = None
        self.script = None
        self.__credentials = None        
    

    def __get_gredentials(self):

        if self.__credentials is not None:
            return self.__credentials
        else:
            config = configparser.ConfigParser()
            config.read_file(open('config/config.cfg'))                                            
            self.__credentials = service_account.Credentials.from_service_account_file(
                config.get('GCP','service-account'))
            
        return self.__credentials

    
    def __get_client(self, typeclient):

        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(self.region)}
        credentials = self.__get_gredentials()
        client = None

        if typeclient == 'cluster':
            client = dataproc.ClusterControllerClient(
                client_options=client_options,
                credentials = credentials
            )
        elif typeclient == 'job':
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
                "master_config": {"num_instances": 1, "machine_type_uri": "custom-1-4096", "disk_config":{"boot_disk_type":"pd-standard", "boot_disk_size_gb":100, "num_local_ssds":1}},
                "worker_config": {"num_instances": 2, "machine_type_uri": "custom-1-4096", "disk_config":{"boot_disk_type":"pd-standard", "boot_disk_size_gb":100, "num_local_ssds":1}},          
            },
        }


        operation = client.create_cluster(
            request={"project_id": self.project_id, "region": self.region, "cluster": cluster}
        )

        result = operation.result()
        print(operation.result)
      
        print(f"Cluster created successfully: {result.cluster_name}")

        return True

                                    
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


    def __submit_job(self, params):

        client = self.__get_client('job')
        
        job = {
            "placement": {"cluster_name": self.cluster_name},          
            "pyspark_job": {"main_python_file_uri": "gs://{}/{}/{}".format(self.bucket, self.folder, self.script + '.py'),
                   "args": ['--params', params],
                },            
            }

        operation = client.submit_job_as_operation(
            request={"project_id": self.project_id, "region": self.region, "job": job}
        )


        print(dir(operation))
        response = operation.result()

        print("ERROR::::", response.driver_output_resource_uri)

        matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

        output = (
            storage.Client()
            .get_bucket(matches.group(1))
            .blob(f"{matches.group(2)}.000000000")
            .download_as_string()
        )

        print(f"Job finished successfully: {output}")
        return output
        

    def run_script(self, script, params):
        
        self.bucket = script[0]
        self.folder = script[1]
        self.script = script[3]

        config = configparser.ConfigParser()
        config.read_file(open('config/config.cfg'))
        self.project_id = config.get('GCP','project')
        self.region = config.get('GCP','region')
        self.cluster_name = 'pydag-cluster-dataproc-{}'.format(time.time_ns())

        if self.__create_cluster():
            self.__submit_job(params)
            self.__delete_cluster()
        else:
            raise DPError('Issues creating cluster : {0}'.format(self.cluster_name))

    
        return True

       
from decimal import DecimalException
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
                "software_config": { "image_version": "1.5-debian10"}
            },
        }


        operation = client.create_cluster(
            request={"project_id": self.project_id, "region": self.region, "cluster": cluster }
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

    
    def __get_url_bucket_output(self, strerror):
        pass


    def __get_output_from_bucket(self, url):
        
        matches = re.match("gs://(.*?)/(.*)", url)
        
        output = (
                    storage.Client()
                    .get_bucket(matches.group(1))
                    .blob(f"{matches.group(2)}.000000000")
                    .download_as_string()
                )
        return output


    def __submit_job(self, params):

        client = self.__get_client('job')
        
        job = {
            "placement": {"cluster_name": 'pydag-cluster-dataproc-1653251777657359700'},          
            "pyspark_job": {"main_python_file_uri": "gs://{}/{}/{}".format(self.bucket, self.folder, self.script + '.py'),
                   "args": ['--params', params],
                   "jar_file_uris": ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
                },            
            }

        try:

            operation = client.submit_job_as_operation(
                request={"project_id": self.project_id, "region": self.region, "job": job}
            )

            response = operation.result()        

            if response.status.state.value == 5:

                output = self.__get_output_from_bucket(response.driver_output_resource_uri)

                print(f"Script { self.script} finished successfully: {output}")

                return True

        except Exception as ex:
            url = self.__get_url_bucket_output(str(ex))
            output = self.__get_output_from_bucket(url)
            raise DPError('Error processing script: {0}:{1}'.format(self.script, output))     
        

    def run_script(self, script, params):
        self.bucket = script[0]
        self.folder = script[1]
        self.script = script[3]

        config = configparser.ConfigParser()
        config.read_file(open('config/config.cfg'))
        self.project_id = config.get('GCP','project')
        self.region = config.get('GCP','region')
        self.cluster_name = 'pydag-cluster-dataproc-{}'.format(time.time_ns())

        # if self.__create_cluster():
        result = self.__submit_job(params)
        #     self.__delete_cluster()
        # else:
        #     raise DPError('Issues creating cluster : {0}'.format(self.cluster_name))

    
        return result

       
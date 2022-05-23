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
import os


class DPClient:

    def __init__(self):
        self.bucket = None
        self.folder = None
        self.script = None
        self.params = None
        self.jars = []
        self.gcp_data = {}
        self.__credentials = None        
    

    def __get_gredentials(self):

        if self.__credentials is not None:
            return self.__credentials
        else:
            config = configparser.ConfigParser()
            config.read_file(open(os.getcwd() + '/config/config.cfg'))                                            
            self.__credentials = service_account.Credentials.from_service_account_file(
                config.get('GCP','service-account'))
            
        return self.__credentials

    
    def __get_client(self, typeclient):

        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(self.gcp_data['region'])}
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
        elif typeclient == 'gcs':
            config = configparser.ConfigParser()
            config.read_file(open(os.getcwd() + '/config/config.cfg'))
            service_account = config.get('GCP','service-account')
            client = storage.Client.from_service_account_json(service_account)
        else:
            pass
        return client        


    def __create_cluster(self):


        client = self.__get_client('cluster')

      
        cluster = {
            "project_id": self.gcp_data['project'],
            "cluster_name": self.gcp_data['dataproc-cluster-name'],
            "config": {
                "master_config": {"num_instances": 1, "machine_type_uri": "custom-1-4096", "disk_config":{"boot_disk_type":"pd-standard", "boot_disk_size_gb":100, "num_local_ssds":1}},
                "worker_config": {"num_instances": 2, "machine_type_uri": "custom-1-4096", "disk_config":{"boot_disk_type":"pd-standard", "boot_disk_size_gb":100, "num_local_ssds":1}},
                "software_config": { "image_version": "1.5-debian10"}
            },
        }


        operation = client.create_cluster(
            request={"project_id": self.gcp_data['project'], "region": self.gcp_data['region'], "cluster": cluster }
        )

        result = operation.result()        
      
        print(f"Cluster created successfully: {result.cluster_name}")

        return True

                                    
    def __delete_cluster(self):

        client = self.__get_client('cluster')

        operation = client.delete_cluster(
            request={
                "project_id": self.gcp_data['project'],
                "region": self.gcp_data['region'],
                "cluster_name": self.gcp_data['dataproc-cluster-name'],
            }
        )
        operation.result()

        print("Cluster {} successfully deleted.".format(self.gcp_data['dataproc-cluster-name']))

    
    def __get_bucket_output(self, strerror):
        result = re.search('gs://(.*)driveroutput', strerror)
        return 'gs://{0}driveroutput'.format(result.group(1))


    def __get_output_from_bucket(self, url):
        
        matches = re.match("gs://(.*?)/(.*)", url)
        
        output = (
                    self.__get_client('gcs')
                    .get_bucket(matches.group(1))
                    .blob(f"{matches.group(2)}.000000000")
                    .download_as_string()
                )
        return output


    def __submit_job(self):

        client = self.__get_client('job')
        
        job = {
            "placement": {"cluster_name": self.gcp_data['dataproc-cluster-name']},          
            "pyspark_job": {"main_python_file_uri": "gs://{}/{}/{}".format(self.bucket, self.folder, self.script + '.py'),
                   "args": ['--params', self.params],                   
                   "jar_file_uris": self.jars,
                },            
            }

        try:

            operation = client.submit_job_as_operation(
                request={"project_id": self.gcp_data['project'], "region": self.gcp_data['region'], "job": job }
            )

            response = operation.result()        

            if response.status.state.value == 5:

                output = self.__get_output_from_bucket(response.driver_output_resource_uri)

                print(f"Script { self.script} finished successfully: {output}")

                return True

        except Exception as ex:
            url = self.__get_bucket_output(str(ex))
            output = self.__get_output_from_bucket(url)
            raise DPError('Error processing script: {0}:{1}'.format(self.script, output))     
        

    def run_script(self, script, params):

        #Location of the script to submit to dataproc
        self.bucket = script[0]
        self.folder = script[1]
        self.script = script[3]

         

        config = configparser.ConfigParser()
        config.read_file(open('config/config.cfg'))

        gcp_config = dict(config.items('GCP'))
        for k, v in gcp_config.items():
            self.gcp_data[k] = v        
        
        self.params = params
        jars = json.loads(params)['spark.jars'].split(',')
        for jar in jars:
            self.jars.append(jar)

        # if self.__create_cluster():
        result = self.__submit_job()
        #     self.__delete_cluster()
        # else:
        #     raise DPError('Issues creating cluster : {0}'.format(self.cluster_name))

    
        return result

       
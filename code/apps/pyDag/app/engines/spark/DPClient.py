import json
from google.oauth2 import service_account
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import configparser
from engines.spark.dperror import DPError
import re
import os


class DPClient:

    def __init__(self, logger):
        self.bucket = None
        self.folder = None
        self.script = None
        self.params = None
        self.jars = []
        self.gcp_data = {}
        self.__credentials = None 
        self.logger = logger       
    

    def __get_gredentials(self):

        if self.__credentials is not None:
            return self.__credentials
        else:                                       
            self.__credentials = service_account.Credentials.from_service_account_file(
                self.gcp_data['GCP_service-account'])
            
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
            client = storage.Client(credentials=self.__get_gredentials())     
        else:
            pass
        return client

    
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
                ).decode()


        lines = output.split("\n")
        output = "\n".join([line for line in lines 
                    if not 'INFO' in line])

        return output


    def __submit_job(self):

        self.logger.info(17, [self.script + '.py', self.gcp_data['cluster_name']])

        client = self.__get_client('job')
        
        job = {
            "placement": {"cluster_name": self.gcp_data['cluster_name']},          
            "pyspark_job": {"main_python_file_uri": "gs://{}/{}/{}".format(self.bucket, self.folder, self.script + '.py'),
                   "args": ['--params', self.params],                   
                   "jar_file_uris": self.jars,
                },
            }

        try:

            operation = client.submit_job_as_operation(
                request={"project_id": self.gcp_data['project_name'], "region": self.gcp_data['region'], "job": job }
            )

            response = operation.result()        

            if response.status.state.value == 5:

                output = self.__get_output_from_bucket(response.driver_output_resource_uri)

                self.logger.info(16,[self.script, output])                

                return True

        except Exception as ex:
            url = self.__get_bucket_output(str(ex))
            output = self.__get_output_from_bucket(url)
            self.logger.info(15,[self.script, output], True, DPError)
        

    def run_script(self, script, params):

        #Location of the script to submit to dataproc
        self.bucket = script[0]
        self.folder = script[1]
        self.script = script[3]

        # params for script
        self.params = params                     

        dict_params = json.loads(self.params)
        
        jars = dict_params['spark.jars'].split(',')

        for jar in jars:
            self.jars.append(jar)

        self.gcp_data = {
            "cluster_name" : "",
            "project_name" : "",
            "region" : "",
            "GCP_service-account": ""
        }

        for k, v in self.gcp_data.items():
            self.gcp_data[k] = dict_params[k]
                
        result = self.__submit_job()
    
        return result
from google.oauth2 import service_account
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import argparse
import os
from pathlib import Path

class Bootstrap:

    def __init__(self, gcp_data):
        self.gcp_data = gcp_data
        self.__credentials = None 
        pass

    def __get_gredentials(self):

        if self.__credentials is not None:
            return self.__credentials
        else:                                     
            self.__credentials = service_account.Credentials.from_service_account_file(os.getcwd() + "/app/config/atomic-key-348412-0c7115c249fb.json")
            
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
            client = storage.Client.from_service_account_file(os.getcwd() + "/app/config/atomic-key-348412-0c7115c249fb.json")
        else:
            pass
        return client     


    def create_cluster(self):

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

                                    
    def delete_cluster(self):

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


    def stop_cluster(self):
        pass


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-a',
                        '--Action', 
                        type=str, 
                        help = "Type of actions", 
                        metavar = '', 
                        choices=['create_cluster',
                                'delete_cluster', 'stop_cluster'])
    
    parser.add_argument('-n','--name', type=str, help = "Cluster name", required=True)    
    parser.add_argument('-p','--project', type=str, help = "Project name", required=True)
    parser.add_argument('-r','--region', type=str, help = "Region name", required=True)    

    args = parser.parse_args()

    gcp_data = {}
    if args.name is not None and args.name!= '':
        if args.project is not None and args.project != '':
            if args.region is not None and args.region != '':
                gcp_data['dataproc-cluster-name'] = args.name
                gcp_data['project'] = args.project
                gcp_data['region'] = args.region
                bootstrap = Bootstrap(gcp_data)

                if args.Action == 'create_cluster':
                    bootstrap.create_cluster()
                elif args.Action == 'delete_cluster':
                    bootstrap.delete_cluster()
                elif args.Action == 'stop_cluster':
                    bootstrap.stop_cluster()       
                else:
                    print("Action {} is invalid".format(args.Action))
            else:
                print("The argument -r or -region is missing")
        else:
            print("The argument -p or -project is missing")
    else:
        print("The argument -n or -name is missing")             
 


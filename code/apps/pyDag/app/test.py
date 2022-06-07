from pydag import pyDag
import json

if __name__ == "__main__":
    jsondag = """{	
        "dag": {
            "dag_id" : "TEST_DAG",
            "script_cache" : true,
            "expire_cache": 7200,
            "schedule_interval": "",
            "tags": ["tag"],
            "description" : "",
            "processors": 4,
            "retries" : 0,
            "owner": "owner",
            "start_date": "", 
            "tasks" : [
                        {
                            "task_id" : "startup_dataproc_1",
                            "script" : "gcs.project-pydag.iac_scripts.iac.dataproc_create_cluster",
                            "params" : "{'cluster_name':'cluster-dataproc-pydag-2022', 'project_name':'atomic-key-348214', 'region':'us-central1', '**GCP_service-account':''}",
                            "dependencies":[]
                        },
                        {
                            "task_id" : "create_table_final",
                            "script" : "gcs.project-pydag.module_name.bq.create_table",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'table':'mytable3', 'columns':'id int64, category string, lastdate date', 'partitioncolumn':'lastdate', 'clustercolumn':'category'}",
                            "dependencies":[]
                        },
                        {
                            "task_id" : "create_table_stg_1",
                            "script" : "gcs.project-pydag.module_name.bq.create_table_stg",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'table':'mytable1', 'columns':'id int64, category string, lastdate date'}",
                            "dependencies":[]
                        },
                        {
                            "task_id" : "create_table_stg_2",
                            "script" : "gcs.project-pydag.module_name.bq.create_table_stg",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'table':'mytable2', 'columns':'id int64, category string, lastdate date'}",
                            "dependencies":[]
                        },  
                        {
                            "task_id" : "initial_ingestion_1",
                            "script" : "gcs.project-pydag.module_name.spark.csv_gcs_to_bq",
                            "params" : "{'cluster_name':'cluster-dataproc-pydag-2022', 'project_name':'atomic-key-348214', 'region':'us-central1', 'dataset':'datasettest', 'destination_table':'table_stg', 'bucket':'project-pydag', 'folder':'module_name', 'file_name':'test.csv', '**GCP_service-account':''}",
                            "dependencies":["startup_dataproc_1", "create_table_final", "create_table_stg_1", "create_table_stg_2"]
                        },                      
                        {
                            "task_id" : "extract_from_stg_1",
                            "script" : "gcs.project-pydag.module_name.bq.extract_from_stg",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'tabledestination': 'mytable1', 'tablesource':'table_stg', 'year':'2022', 'category':'cars'}",
                            "dependencies":["initial_ingestion_1"]
                        },
                        {
                            "task_id" : "extract_from_stg_2",
                            "script" : "gcs.project-pydag.module_name.bq.extract_from_stg",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'tabledestination': 'mytable2', 'tablesource':'table_stg', 'year':'2021', 'category':'food'}",
                            "dependencies":["initial_ingestion_1"]
                        },
                        {
                            "task_id" : "insert_to_fact",
                            "script" : "gcs.project-pydag.module_name.bq.insert_to_fact",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'table1': 'mytable1','table2': 'mytable2', 'tablefinal':'mytable3'}",
                            "dependencies": ["extract_from_stg_1", "extract_from_stg_2"]
                        }
            ]
        }
    }"""


    datadag = json.loads(jsondag)
    pydag = pyDag(4,"TEST_DAG")
    print("adding dag")
    pydag.addDag(datadag)
    print("running dag")
    pydag.run()
    print("Dag completed")
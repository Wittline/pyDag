from pydag import pyDag
import json
if __name__ == "__main__":
    jsondag = """{	
        "dag": {
            "dag_id" : "DAG-TEST",
            "script_cache" : true,
            "expire_cache": 7200,
            "schedule_interval": "",
            "catchup":false,
            "tags": ["tag"],
            "description" : "",
            "concurrency": 1,
            "max_active_runs": 1,
            "dagrun_timeout_sec": 600,
            "default_args": {
                    "retries" : 0,
                    "retry_delay_sec" : 300,
                    "owner": "default_owner",
                    "start_date": "2020-01-01T00:00:00.000Z"
            }, 
            "tasks" : [
                        {
                            "task_id" : "initial_ingestion",
                            "script" : "gcs.project-pydag.module_name.spark.csv_gcs_to_bq",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'destination_table':'table_stg', 'bucket':'project-pydag', 'folder':'module_name', 'file_name':'test.csv'}",
                            "dependencies":[]
                        },
                        {
                            "task_id" : "create_table_final",
                            "script" : "gcs.project-pydag.module_name.bq.create_table",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'table':'mytable3', 'columns':'id int64, category string, lastdate date', 'partitioncolumn':'lastdate', 'clustercolumn':'category'}",                            
                            "dependencies":["initial_ingestion"]
                        },
                        {
                            "task_id" : "create_table_stg_1",
                            "script" : "gcs.project-pydag.module_name.bq.create_table_stg",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'table':'mytable1', 'columns':'id int64, category string, lastdate date'}",
                            "dependencies":["initial_ingestion"]
                        },
                        {
                            "task_id" : "create_table_stg_2",
                            "script" : "gcs.project-pydag.module_name.bq.create_table_stg",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'table':'mytable2', 'columns':'id int64, category string, lastdate date'}",
                            "dependencies":["initial_ingestion"]
                        },                        
                        {
                            "task_id" : "extract_from_stg_1",
                            "script" : "gcs.project-pydag.module_name.bq.extract_from_stg",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'tabledestination': 'mytable1', 'tablesource':'table_stg', 'year':'2022', 'category':'cars'}",
                            "dependencies":["create_table_stg_1"]
                        },
                        {
                            "task_id" : "extract_from_stg_2",
                            "script" : "gcs.project-pydag.module_name.bq.extract_from_stg",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'tabledestination': 'mytable2', 'tablesource':'table_stg', 'year':'2021', 'category':'food'}",
                            "dependencies":["create_table_stg_2"]
                        },
                        {
                            "task_id" : "insert_to_fact",
                            "script" : "gcs.project-pydag.module_name.bq.insert_to_fact",
                            "params" : "{'project':'atomic-key-348214', 'dataset':'datasettest', 'table1': 'mytable1','table2': 'mytable2', 'tablefinal':'mytable3'}",
                            "dependencies": ["create_table_final", "extract_from_stg_1", "extract_from_stg_2"]
                        }



            ]
        }
    }"""

    datadag = json.loads(jsondag)
    pydag = pyDag(3)
    pydag.addDag(datadag)
    print(pydag.run())
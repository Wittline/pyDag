# pyDag


#### Check the article here:  <a href="https://itnext.io/how-to-build-a-dag-based-task-scheduling-tool-for-multiprocessor-systems-using-python-d11a093a835b">How to build a DAG based Task Scheduling tool for Multiprocessor systems using python</a>

## Scheduling Big Data Workloads and Data Pipelines in the Cloud with pyDag

<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/172307778-fd7f1449-0ed8-428a-8bf2-601e997a7c9f.png"
  >
</p>

### pyDag's Architecture for a Multiprocessor Machine.
An object of the pyDag class contains everything mentioned below, this is an whole overview of the architecture.

<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/172308286-416ac520-6c81-4911-8e6d-b0e307c1d270.png"
  >
</p>

### Let's run an example

#### GCP - API credentials in JSON
- Go to the Google Cloud console
- Create a new project
- Follow the steps in this video to create Api Credentials in Json :

#### GCP - BigQuery
- Go to BigQuery
- Create a new dataset called: datasettest

<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/172309959-b580b149-913c-4ca9-bb36-10fc434086e1.png"
  >
</p>

#### GCP - Dataproc
- Go to Dataproc
- Now click on Enable API


<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/172310472-8d0024a1-a6c6-4d42-a5f5-f973a1605fec.png"
  >
</p>

#### LOCAL MACHINE
- Install Docker Desktop on Windows, it will install Docker Compose as well, Docker Compose will allow you to run multiple container applications.
- Install git-bash for windows, once installed, open git bash and download this repository, this will download the docker-compose.yaml file, and other files needed.

```linux 
ramse@DESKTOP-K6K6E5A MINGW64 /c
$ git clone https://github.com/Wittline/pyDag.git
```

- Once all the files needed were downloaded from the repository, let's run everything. We will use the git bash tool again, go to the folder pyDag and we will run the Docker Compose command:

```linux

ramse@DESKTOP-K6K6E5A MINGW64 /c
$ cd pyDag

ramse@DESKTOP-K6K6E5A MINGW64 /c/pyDag
$ cd code

ramse@DESKTOP-K6K6E5A MINGW64 /c/pyDag/code
$ cd apps

@DESKTOP-K6K6E5A MINGW64 /c/pyDag/code/apps
$ docker-compose up

```
- Go to the logs folder and check the output

### Let's explain the example
There are many configurations for the DAG that could work for this example, the most appropriate and the shortest is the second approach shown in the image below, I discarded the first approach, both approaches achieve the same goal, but, with the second approach there is more chances to take advantage of the parallelism and improve the overall latency.

<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/172311381-c308acf3-8c86-42cf-968b-b64acb7b133e.png"
  >
</p>

This example is just to demonstrate that this tool can reach various levels of granularity, the example can be built in fewer steps, in fact using a single query against BigQuery, but it is a very simple example to see how it works.

<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/172311527-e27d1827-89c9-4661-9455-61d4d8b44085.png"
  >
</p>

- **startup_dataproc_1**: Create a Dataproc cluster in GCP with the name: "cluster-dataproc-pydag-2022".
- **create_table_final**: Create the final table "mytable3" in BigQuery, here we want the data chosen and cleaned.
- **create_table_stg_1**: Create the table "mytable1" in BigQuery.
- **create_table_stg_2**: Create the table "mytable2" in BigQuery.
- **initial_ingestion_1**: This task will move the data from the .CSV file stored in Google cloud Storage to BigQuery, this script will be executed in the already created Dataproc cluster with the task: "startup_dataproc_1". In this case the Dataproc cluster will work as a ingestion layer, the destination table is called "table_stg".
- **extract_from_stg_1**: This task will move the data from the "table_stg" to "mytable1" only will take records with the following conditions: 'year':'2022' and 'category':'cars'
- **extract_from_stg_2**: This task will move the data from the "table_stg" to "mytable2" only will take records with the following conditions: 'year':'2021' and 'category':'food'.
- **insert_to_fact**: This task will populate the data from "mytable1" and "mytable2" to "mytable3" using an UNION ALL.


##### Checking Tables After the execution

<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/172311916-2a99d33a-4fb2-45bc-b420-37098847c3c6.png"
  >
</p>

##### Checking logs After the execution
<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/172312006-763e71ed-12c5-4b23-95b1-9c68df0ed352.png"
  >
</p>

### Next steps
In order to have an acceptable product with the minimum needed features, I will be working on adding the following:
- Centralized logs
- A metadata database
- Distributed Task Execution on multiple machines
- Branching
- DAG as a Service using an API REST.
- GUI with drag and drop components

### Conclusions
You can clearly observe that in all cases there are two tasks taking a long time to finish "**startup_dataproc_1**" and "**initial_ingestion_1**" both related with the use of Google DataProc, one way to avoid the use of tasks that create Clusters in DataProc is by keeping an already cluster created and keeping it turned on waiting for tasks, with horizontally scaling, this is highly recommended for companies that has a high workloads by submitting tasks where there will be no gaps of wasted and time and resources.

You can see the effect of the caching in the executions, short tasks are shorter in cases where the cache is turned on.

<p align="center">
  <img 
    src="https://user-images.githubusercontent.com/8701464/172312420-cbb88a27-2cd1-4922-9281-82dc0103ae3d.png"
  >
</p>

Although the parallelism in tasks execution can be confirmed, we could assign a fixed number of processors per DAG, which represents the max number of tasks that could be executed in parallel in a DAG or **maximum degree of parallelism**, but this implies that sometimes there are processors that are being wasted, one way to avoid this situation is by assigning a dynamic number of processors, that only adapts to the number of tasks that need to be executed at the moment, in this way multiple DAGS can be executed on one machine and take advantage of processors that are not being used by other DAGS. The only issue with the above chart is that these results coming from one execution for each case, multiple executions should be done for each case and take an average time on each case, but I don't have the enough budget to be able to do this kind of tests, the code is still very informal, and it's not ready for production, I'll be working on these details in order to release a more stable version.

## Contributing and Feedback
Any ideas or feedback about this repository?. Help me to improve it.

## Authors
- Created by <a href="https://twitter.com/RamsesCoraspe"><strong>Ramses Alexander Coraspe Valdez</strong></a>
- Created on 2022

## License
This project is licensed under the terms of the Apache License.

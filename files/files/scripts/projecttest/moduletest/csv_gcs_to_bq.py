import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json

class SparkTask:

    def __init__(self, params):
        self.params = params        

    def create_spark_session(self):
    
        spark = SparkSession\
            .builder\
            .master('yarn')\
            .appName(self.params['id'])\
            .getOrCreate()

        return  spark
    
    def execute_task(self, spark):

            df = spark\
                .read\
                .option("inferSchema","true")\
                .option("header","true")\
                .csv("gs://{}/{}/{}".format(
                    self.params['bucket'],
                    self.params['folder'], 
                    self.params['file_name']))

            df = df.withColumnRenamed("col1","id")\
                .withColumnRenamed("col2","category")\
                .withColumnRenamed("col3","lastdate")        

            
            df.write.format('bigquery') \
                .option('table', '{}.{}'.format(self.params['dataset'], self.params['destination_table'])) \
                .mode("overwrite") \
                .save()

            return True


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--params', type=str)
    args = parser.parse_args()

    params = json.loads(args.params)

    st = SparkTask(params)
    spark = st.create_spark_session()

    spark.conf.set('temporaryGcsBucket', params['temporarygcsbucket'])

    if st.execute_task(spark):
        print(params['id'] + " --> Ready")

    spark.stop()

    
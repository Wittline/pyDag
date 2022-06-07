import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType
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

        schema = StructType() \
            .add("col1",IntegerType(),True) \
            .add("col2",StringType(),True) \
            .add("col3",DateType(),True)
            
        df = spark\
                .read\
                .schema(schema) \
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

    
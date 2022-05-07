dummy_script = """
valor1 = {valor1}
valor2 = {valor2}
valor3 = {valor3}
"""

class sqlScripts:

    def __init__(self):               
        self.scripts = { 
            'dummy': dummy_script
            }
    
    def get_script(self, scr_id, params):        
        scr_id = scr_id.lower()
        if scr_id in self.scripts:
            return self.scripts[scr_id].format(**params)
        else:
            return ''

# import configparser
# config = configparser.ConfigParser()
# config.read_file(open('config.cfg'))

# column = config.get('COLUMN_NORMALIZATION','COLUMN')
# db_ip = config.get('POSTGRESQL', 'IP')
# db_port = config.get('POSTGRESQL', 'PORT')

# column_normalization = """
# import json
# from pyspark.sql.functions import udf, col, explode
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
# from pyspark.sql import Row
# from pyspark import SparkFiles
# from pyspark.sql import SparkSession

# db_ip = '{}'
# db_port = '{}'
# column = '{}'


# url_db = "jdbc:postgresql://localhost:5432/db"

# db_csv = spark.read.format("jdbc").option("url", "jdbc:postgresql://pg_container:5432/db").option("dbtable", "dataset") .option("user", "postgres").option("password", "12345").option("driver", "org.postgresql.Driver").load()
    
# """

# column_normalization_script = column_normalization.format(db_ip, db_port, column)

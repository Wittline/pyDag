from pysparksession import PySparkSession
from scalasparksession import ScalaSparkSession
from sparksession import SparkSession
from sparksessionerror import SparkSessionError
import configparser


class LivyClient:

    def __init__(self, id):
        self.id = id

    def __get_server_url(self):
        config = configparser.ConfigParser()
        config.read_file(open('config/config.cfg'))
        return config.get('livy','ip-port')

    def __sparksession(self):
        return SparkSession(livy_server_url=self.__get_server_url(),kind='pyspark')

    def __pysparksession(self):
        return PySparkSession()
    
    def __scalasession(self):
        return ScalaSparkSession()  

    def run_script(self, script):
        try:
            result = self.__sparksession.run(script)
            if result is not None and result:
                return True
            else:
                return False
        except SparkSessionError as e:
            raise

        
        


        
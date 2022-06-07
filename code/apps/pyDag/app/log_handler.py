import logging
import os
from code_errors import CodeErrors
import os
import configparser
from google.cloud import storage, exceptions


class LogHandler():


    def __init__(self, logger_name, filename, level = 'DEBUG'):
        
        self.log_format = "%(asctime)s::%(levelname)s::%(name)s::"\
              "%(filename)s::%(lineno)d::%(message)s"
        

        dir_dn = filename.split('/')
        self.filename = dir_dn[2]
        self.sub_folder = '/'.join(dir_dn[0:2]) + '/' + self.filename

        self.path = os.getcwd() + "/logs/" + self.sub_folder
        
        dn = os.path.dirname(self.path)
        if not os.path.exists(dn):
            os.makedirs(dn)

        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(level)
        self.filehandler = logging.FileHandler(self.path)
        self.formatter = logging.Formatter(self.log_format)
        self.filehandler.setFormatter(self.formatter)
        self.logger.addHandler(self.filehandler)            

    def close(self):                
        try:
            config = configparser.ConfigParser()
            config.read_file(open(os.getcwd() + '/app/config/config.cfg'))
            service_account = config.get('GCP','service-account')
            bucket_name = config.get('log','bucket')
            folder_name = config.get('log','folder')
            storage_client = storage.Client.from_service_account_json(service_account)
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(folder_name + '/' + self.sub_folder)
            blob.upload_from_filename(self.path, content_type='text/plain')           
        except exceptions.GoogleCloudError as ex:
            raise

    def info(self, code, params, raise_error = False, error_type = None):
        error_msg = CodeErrors[code].format(*params)
        self.logger.info(error_msg)
        if raise_error:
            self.close()
            raise error_type(error_msg)
            

    

    


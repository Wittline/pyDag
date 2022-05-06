import json
class ScriptHandler:

    def __init__(self, id,  script, params):
        self.id = id
        self.script = script
        self.params = params

    def __get_connections(self, p_dict):
        return {k: v for k, v in p_dict.items() if k[0:2] == '**'}

    def __get_variables(self, p_dict):
         return {k: v for k, v in p_dict.items() 
            if k[0:2] != '**' and  k[0] == '*' }

    def __get_parameters(self, p_dict):
        return {k: v for k, v in p_dict.items() 
            if k[0] != '*'}                            

    def __get_script(self, scr):
        pass        

    def format_script(self):
        _path = self.script.split('.')
        if len(_path) == 4:
            script = self.__get_script(_path)
            p_dict  = json.loads(self.params.replace("'",'"'))
            params = self.__get_parameters(p_dict)
            connections = self.__get_connections(p_dict)
            variables = self.__get_variables(p_dict)
        else:
            raise Exception("Path incomplete for script {0} in task id {1}".format(self.script, self.id))

    

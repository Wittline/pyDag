dummy_script = """

valor1 = {valor1}
valor2 = {valor2}
valor3 = {valor3}


"""

class pyScripts:

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

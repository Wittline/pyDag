script_create_table_stg = """
CREATE OR REPLACE TABLE '{project}.{dataset}.{table}'
(
    {columns}    
)
;
"""

class create_table_stg:

    def __init__(self):               
        self.scripts = { 
            'create_table_stg': script_create_table_stg,      
            }
    
    def get_script(self, scr_id, params):        
        scr_id = scr_id.lower()
        if scr_id in self.scripts:
            return self.scripts[scr_id].format(**params)
        else:
            return ''
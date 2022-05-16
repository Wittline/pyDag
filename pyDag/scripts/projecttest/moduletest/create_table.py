script_create_table = """
CREATE OR REPLACE TABLE {project}.{dataset}.{table}
(
    {columns}    
)
PARTITION BY {partitioncolumn}
CLUSTER BY {clustercolumn}
;
"""


class create_table:

    def __init__(self):               
        self.scripts = { 
            'create_table': script_create_table,      
            }
                
    def get_script(self, scr_id, params):        
        scr_id = scr_id.lower()
        if scr_id in self.scripts:
            return self.scripts[scr_id].format(**params)
        else:
            return ''
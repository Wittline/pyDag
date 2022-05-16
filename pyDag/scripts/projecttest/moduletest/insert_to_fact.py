script_insert_to_fact =  """
insert into {project}.{dataset}.{tablefinal}
(id, category, lastdate)
select id, category, lastdate
from {project}.{dataset}.{table1}
union all
select id, category, lastdate
from {project}.{dataset}.{table2}
"""

class insert_to_fact:

    def __init__(self):               
        self.scripts = { 
            'insert_to_fact': script_insert_to_fact,      
            }
    
    def get_script(self, scr_id, params):        
        scr_id = scr_id.lower()
        if scr_id in self.scripts:
            return self.scripts[scr_id].format(**params)
        else:
            return ''
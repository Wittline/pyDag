script_extract_from_stg = """
insert into {project}.{dataset}.{tabledestination}
(id, category, lastdate)
select id, category, lastdate
from {project}.{dataset}.{tablesource}
WHERE EXTRACT(YEAR FROM lastdate) = {year} and category = '{category}'
"""


class extract_from_stg:

    def __init__(self):               
        self.scripts = { 
            'extract_from_stg': script_extract_from_stg,      
            }
    
    def get_script(self, scr_id, params):        
        scr_id = scr_id.lower()
        if scr_id in self.scripts:
            return self.scripts[scr_id].format(**params)
        else:
            return ''
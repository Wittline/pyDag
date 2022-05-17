insert into {project}.{dataset}.{tabledestination}
(id, category, lastdate)
select id, category, lastdate
from {project}.{dataset}.{tablesource}
WHERE EXTRACT(YEAR FROM lastdate) = {year} and category = '{category}'

insert into {project}.{dataset}.{tablefinal}
(id, category, lastdate)
select id, category, lastdate
from {project}.{dataset}.{table1}
union all
select id, category, lastdate
from {project}.{dataset}.{table2}

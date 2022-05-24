CREATE OR REPLACE TABLE {project}.{dataset}.{table}
(
    {columns}    
)
PARTITION BY {partitioncolumn}
CLUSTER BY {clustercolumn}
;
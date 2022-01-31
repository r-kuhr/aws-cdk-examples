import sys
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)

# NOTE: Args are changed from kebob case to snake case with `getResolvedOptions`
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source-path', 'source-db', 'source-table', 'output-path', 'output-db', 'output-table',
])
source_path = args['source_path']
source_db = args['source_db']
source_table = args['source_table']
output_path = args['output_path']
output_db = args['output_db']
output_table = args['output_table']

dynamicF = glueContext.create_dynamic_frame.from_catalog(
    database = source_db,
    table_name = source_table,
    transformation_ctx = "dynamicF")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options = {
                "paths": [source_path],
                "recurse" : True,
            },
            format='json',
            transformation_ctx = "datasource0",
            )

def MergePartitions(rec):
    dt = datetime.strptime(rec["timestamp"], '%Y-%m-%d %H:%M:%S')
    rec["month"] = dt.month
    rec["year"] = dt.year
    rec["event_name"] = rec["event_type"]
    return rec

mapped_dyf = source_dyf.map(MergePartitions).repartition(1)

datasink = glueContext.write_dynamic_frame.from_options(
    frame = mapped_dyf,
    connection_type = "s3",
    connection_options = {
        "path": output_path,
        "partitionKeys": ["event_name", "year", "month"],
    },
    format = "glueparquet",
    transformation_ctx = "datasink0",
)

job.commit()

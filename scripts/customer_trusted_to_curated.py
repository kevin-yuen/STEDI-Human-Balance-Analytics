import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_trusted
customer_trusted_node1773500569139 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1773500569139")

# Script generated for node accelerometer_landing
accelerometer_landing_node1773500629516 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1773500629516")

# Script generated for node Filter trusted customers with accelerometer
SqlQuery1688 = '''
select distinct(c.email), c.*
from customer_trusted c
    inner join accelerometer_landing a
    on c.email = a.user
'''
Filtertrustedcustomerswithaccelerometer_node1773501340803 = sparkSqlQuery(glueContext, query = SqlQuery1688, mapping = {"customer_trusted":customer_trusted_node1773500569139, "accelerometer_landing":accelerometer_landing_node1773500629516}, transformation_ctx = "Filtertrustedcustomerswithaccelerometer_node1773501340803")

# Script generated for node customer_curated
customer_curated_node1773500686829 = glueContext.write_dynamic_frame.from_catalog(frame=Filtertrustedcustomerswithaccelerometer_node1773501340803, database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1773500686829")

job.commit()
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
customer_trusted_node1773501876185 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1773501876185")

# Script generated for node accelerometer_landing
accelerometer_landing_node1773501876710 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1773501876710")

# Script generated for node step_trainer_landing
step_trainer_landing_node1773501877558 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1773501877558")

# Script generated for node Filter trusted customers with accelerometer
SqlQuery1819 = '''
select distinct(c.email), c.*
from customer_trusted c
    inner join accelerometer_landing a
    on c.email = a.user
'''
Filtertrustedcustomerswithaccelerometer_node1773501955663 = sparkSqlQuery(glueContext, query = SqlQuery1819, mapping = {"customer_trusted":customer_trusted_node1773501876185, "accelerometer_landing":accelerometer_landing_node1773501876710}, transformation_ctx = "Filtertrustedcustomerswithaccelerometer_node1773501955663")

# Script generated for node Filter step trainer for trusted customers with accelerometer
SqlQuery1818 = '''
select s.*
from customers_trusted_accelerometer c
    inner join step_trainer_landing s
    on c.serialNumber = s.serialNumber
'''
Filtersteptrainerfortrustedcustomerswithaccelerometer_node1773502126074 = sparkSqlQuery(glueContext, query = SqlQuery1818, mapping = {"customers_trusted_accelerometer":Filtertrustedcustomerswithaccelerometer_node1773501955663, "step_trainer_landing":step_trainer_landing_node1773501877558}, transformation_ctx = "Filtersteptrainerfortrustedcustomerswithaccelerometer_node1773502126074")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1773502294210 = glueContext.write_dynamic_frame.from_catalog(frame=Filtersteptrainerfortrustedcustomerswithaccelerometer_node1773502126074, database="stedi", table_name="step_trainer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="step_trainer_trusted_node1773502294210")

job.commit()
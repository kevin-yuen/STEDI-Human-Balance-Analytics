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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1773502963838 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1773502963838")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1773502981977 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1773502981977")

# Script generated for node Filter step trainer with accelerometer and trusted customers
SqlQuery1626 = '''
select * 
from step_trainer_trusted s
    inner join accelerometer_trusted a
    on s.sensorReadingtime == a.timestamp

'''
Filtersteptrainerwithaccelerometerandtrustedcustomers_node1773503011587 = sparkSqlQuery(glueContext, query = SqlQuery1626, mapping = {"step_trainer_trusted":step_trainer_trusted_node1773502963838, "accelerometer_trusted":accelerometer_trusted_node1773502981977}, transformation_ctx = "Filtersteptrainerwithaccelerometerandtrustedcustomers_node1773503011587")

# Script generated for node machine_learning_curated
machine_learning_curated_node1773503242663 = glueContext.write_dynamic_frame.from_catalog(frame=Filtersteptrainerwithaccelerometerandtrustedcustomers_node1773503011587, database="stedi", table_name="machine_learning_curated", transformation_ctx="machine_learning_curated_node1773503242663")

job.commit()
from pyspark.sql.session import *
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from datetime import datetime

# Creating SparkSession and SparkContext
spark = (SparkSession.builder
         .appName("Usecase-5")
         .enableHiveSupport()
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Default configuration for Hadoop/GCS connectivity
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# Custom schema creation, reading data from GCS bucket using Pyspark and creating a dataframe
custs_schema = StructType([StructField("custid",IntegerType(),True),
                          StructField("firstname",StringType(),True),
                          StructField("lastname",StringType(),True),
                          StructField("age",IntegerType(),True),
                          StructField("profession",StringType(),True)])

# Creating dateformat for validation
current_datetime = datetime.now()
formatted_datetime = current_datetime.strftime('%Y%m%d%H')

# Performing EL (Extract Load) operation:
# Extracting GCS daily data
gcs_daily_data = f"gs://inceptez-usecase5/custs_{formatted_datetime}"
custs_gcs = spark.read.csv(gcs_daily_data,sep=",",schema=custs_schema)

# Load data into Bigquery
(custs_gcs.write.format("bigquery")
 .option("temporaryGcsBucket",'incpetez-usecase5/tmp')
 .option("table","rawdataset.cust_raw")
 .save())

#Transformations:
custs_gcs.createOrReplaceTempView("custs_view") # Temp view create to write SQL type queries
transformed_custs_gcs = spark.sql("select profession, round(avg(age),2) as avg_age from custs_view group by profession order by avg_age DESC")
(transformed_custs_gcs.write
 .format("bigquery")
 .option("temporaryGcsBucket",'incpetez-usecase5/tmp')
 .option("table","curatedataset.curated_table")
 .save())

# Stopping the program
spark.stop()

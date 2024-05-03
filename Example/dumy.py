import os
import sys
import pyspark
from pyspark import SparkConf
from pyspark.shell import sc
from pyspark.sql import SparkSession

# os.environ['PYSPARK_PYTHON']=sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable
# print(sys.executable)

cluster_master = "spark://https://storage.cloud.google.com/dataproc-staging-africa-south1-865829957619-cqehltlw/google-cloud-dataproc-metainfo/67961276-133d-43a5-a146-e21c845efb26/cluster.properties:7077"

# Create a SparkSession
# spark = SparkSession.builder \
#     .appName("MySparkApp") \
#     .master(cluster_master) \
#     .getOrCreate()

conf = SparkConf().setAppName("testing").setMaster(cluster_master)

#conf = SparkConf().setMaster(cluster_master)
sc = pyspark.SparkContext(conf = conf)

print("Spark session created successfully."+ sc)
# # Creating RDD using parallelize method of SparkContext
# rdd = sc.parallelize(spark)
#
# #Returning distinct elements from RDD
# distinct_numbers = rdd.distinct().collect()
#
# #Print
# print('Distinct Numbers:', distinct_numbers)
if not sc._jsc.sc().isStopped():
  sc.stop()


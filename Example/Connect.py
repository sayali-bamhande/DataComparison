# Remove or comment out this line if you don't want the Python version and platform to be printed
# import sys; print('Python %s on %s' % (sys.version, sys.platform))
import os
import sys

from pyspark.shell import sc
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON']=sys.executable
os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable
print(sys.executable)

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .master("local[*]") \
    .getOrCreate()

print("Spark session created successfully.")
# Don't forget to stop the SparkSession when done

import threading
def timer_elapsed():
    print('Timer elapsed')
    if not sc._jsc.sc().isStopped():
        spark.stop()

# Set the timer to stop the Spark session after 10 seconds
timer = threading.Timer(10, timer_elapsed)
timer.start()
print("Spark session is stopped")
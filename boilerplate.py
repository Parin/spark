import os
import sys
import pprint

# Path for spark source folder
os.environ['SPARK_HOME'] = "/Users/parin/lib/spark-1.4.0"

# Append pyspark to Python Path
sys.path.append("/Users/parin/lib/spark-1.4.0/python/")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

SPARK_MODE = "local"
APP_NAME = "boilerplate"
INPUT_DIR = "/Users/parin/PycharmProjects/spark/data"


def process_values(kv):
    contents = kv[1]
    lines = contents.split("\n")
    new_kv = (kv[0], [])
    for line in lines:
        fields = line.split(",")
        if fields[0] != "sueid":
            new_kv[1].append(line)
    return new_kv

conf = SparkConf().setMaster(SPARK_MODE).setAppName(APP_NAME)
sc = SparkContext(conf=conf)

input_files = sc.wholeTextFiles(INPUT_DIR)
pprint.pprint(input_files.map(process_values).collect())

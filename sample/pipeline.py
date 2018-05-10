from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()


rdd = spark.sparkContext.parallelize([[1, "a"], [2, "b"], [3, "c"], [4, "d"]])
a = spark.createDataFrame(rdd, ['ind', "state"])
a.show()
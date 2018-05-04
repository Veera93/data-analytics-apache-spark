from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("Count")
    sc = SparkContext(conf= conf)
    input = ["scala", "java", "hadoop", "spark", "akka", "spark vs hadoop", "pyspark", "pyspark and spark"]
    words = sc.parallelize(input)
    print("Number of elements -> %i which are %s" % (words.count(), words.collect()))
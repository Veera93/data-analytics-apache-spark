from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("Sample")
    sc = SparkContext(conf = conf)

    logFile = "../installation.txt"
    logData = sc.textFile(logFile).cache()

    numAs = logData.filter(lambda s: 'a' in s).count()
    numBs = logData.filter(lambda s: 'b' in s).count()

    print ("Lines with a: %i, lines with b: %i" % (numAs, numBs))

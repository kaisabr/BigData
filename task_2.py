from pyspark import SparkContext, SparkConf
import tsv


rdd = None
rdd_sample = None

conf = SparkConf()
sc = SparkContext(conf = conf)

#Initializes RDD
def createRDD(filename, val):
    rdd = sc.textFile(filename).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample

def sortCount(rdd):
    rddCount = rdd.map(lambda x: x[1]).countByValue().items()
    rddSorted = sorted(rddCount, key = lambda x: (x[1]*(-1), x[0]))
    return rddSorted


def saveAsTextFile(filename, rdd):
        writer = tsv.TsvWriter(open(filename, "w"))
        for country, count in rdd:
            writer.line(country + "\t" + str(count))
        writer.close()

def mainTask2():
    rdd = createRDD("/Users/vilde/BigData/data/geotweets.tsv", 0.1)
    rddSorted = sortCount(rdd)
    saveAsTextFile("/Users/vilde/BigData/result_2.tsv", rddSorted)

mainTask2()

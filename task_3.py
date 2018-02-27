from pyspark import SparkContext, SparkConf
import tsv


rdd = None
rdd_sample = None
conf = SparkConf()
#conf.setMaster("local")
#conf.setAppName("My application")
#conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf = conf)

#Initializes RDD
def createRDD(filename, val):
    rdd = sc.textFile(filename).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample



def saveAsTextFile(filename, rdd):
        writer = tsv.TsvWriter(open(filename, "w"))
        writer.close()

def mainTask3():
    rdd = createRDD("/Users/vilde/BigData/data/geotweets.tsv", 0.1)
    saveAsTextFile("/Users/vilde/BigData/result_3.tsv", rdd)

mainTask3()

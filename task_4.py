from pyspark import SparkContext, SparkConf
import tsv
from datetime import datetime as dt

#conf.setMaster("local")
#conf.setAppName("My application")
#conf.set("spark.executor.memory", "1g")

rdd = None
rdd_sample = None
conf = SparkConf()

sc = SparkContext(conf = conf)
sc.setLogLevel("OFF")

#Initializes RDD
def createRDD(filename, val):
    rdd = sc.textFile(filename).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample


def findLocalTime(rdd):
    #a list of sorted timestamp objects:
    localTimeInSeconds = sorted(rdd.map(lambda x: (dt.fromtimestamp((float(x[0]) / 1000) + float(x[8])))).collect())
    #create a list of only hours, not the entire timestamp object
    hourIntervals = []
    for x in range(0,len(localTimeInSeconds)-1):
        #is there a smarter way to convert from list of
        # timestamp object to only hours than a for loop?
        hourIntervals.append(localTimeInSeconds[x].hour)

    from collections import Counter
    import operator
    #creates a dictionary of the hour intervals:
    hourDict = (Counter(hourIntervals))
    #finds the key in hourDict with the maximum value:
    maxValue = hourDict.most_common(1) # gives (key:value)
    #alternative formulation:
    #maxValue= (max(hourDict.iteritems(), key=operator.itemgetter(1))[0])
    return maxValue

def mainTask4():
    rdd = createRDD("/Users/kaisarokne/git/BigData/geotweets.tsv", 1)
    print findLocalTime(rdd)
    #saveAsTextFile("/Users/kaisarokne/git/BigData/result_4.tsv", rdd, df)


mainTask4()

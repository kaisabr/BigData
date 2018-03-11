from pyspark import SparkContext, SparkConf
from datetime import datetime as dt

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

def findMostFrequentHour(rdd):
    from collections import Counter
    # a list of timestamp objects per country:
    localTimeInSeconds = rdd.map(lambda x: (x[1], dt.fromtimestamp((float(x[0]) / 1000) + int(x[8]))))
    #intervals = an array consisting of country name and a Counter dictionary of hour intervals and their frequency on the form: [(country name, Counter({hour: frequency, ...})),...]
    #selecting hour with the highest frequency by most_common(1):
    intervals = localTimeInSeconds.map(lambda x: (x[0], x[1].hour)).groupByKey().mapValues(lambda x: Counter(x).most_common(1)).collect()
    mostFrequent = sc.parallelize(intervals).map(lambda (x, y): str(x) + "\t" + str(y[0][0]) + "\t"+ str(y[0][1]))
    #mostFrequent = country name <tab> most frequent hour <tab> number of tweets
    return mostFrequent

def mainTask4():
    rdd = createRDD("./geotweets.tsv", 1)
    mostFrequentHour = findMostFrequentHour(rdd)
    mostFrequentHour.coalesce(1).saveAsTextFile("./result_4.tsv")
mainTask4()
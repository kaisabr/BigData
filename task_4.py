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
    from collections import Counter
    # a list of timestamp objects per country:
    localTimeInSeconds = rdd.map(lambda x: (x[1], dt.fromtimestamp((float(x[0]) / 1000) + int(x[8]))))
    #Array consisting of country name and a Counter dictionary of hour intervals and their frequency: [(country name, Counter({hour: frequency, ...})),...]
    intervals = localTimeInSeconds.map(lambda x: (x[0], x[1].hour)).groupByKey().mapValues(lambda x: Counter(x)).collect()

    output = ""
    for x in range(0,len(intervals)):
        #find the most frequent hour in the Counter-dictionary:
        mostFrequentHours = intervals[x][1].most_common(1)
        output += str(intervals[x][0]) + "\t" + str(mostFrequentHours[0][0]) + "\t" + str(mostFrequentHours[0][1]) + "\n"
        #output = country name <tab> most frequent hour <tab> number of tweets
    return output

def mainTask4():
    rdd = createRDD("/Users/kaisarokne/git/BigData/geotweets.tsv", 0.0001)
    print findLocalTime(rdd)
    #saveAsTextFile("/Users/kaisarokne/git/BigData/result_4.tsv", rdd, df)

mainTask4()
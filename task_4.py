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
    #a list of sorted timestamp objects per country:
    localTimeInSeconds = sorted(rdd.map(lambda x: (x[1], dt.fromtimestamp((float(x[0]) / 1000) + float(x[8])))).collect())
    for x in range (0,3):
        print localTimeInSeconds[x][1].hour

    #create dictionary dict = {(countryName, hour): tweetCount}

    #create a list of only hours, not the entire timestamp object
    hourIntervals = []
    '''for x in range(0,5):
        #is there a smarter way to convert from list of
        # timestamp object to only hours than a for loop?
        hourIntervals.append(localTimeInSeconds[x].hour)
    print hourIntervals'''

    from collections import Counter
    #creates a dictionary of the hour intervals:
    hourDict = (Counter(hourIntervals))
    print hourDict
    #finds the key in hourDict with the maximum value:
    maxValue = hourDict.most_common(1) # gives (key:value)
    print maxValue
    return maxValue

def mainTask4():
    rdd = createRDD("/Users/kaisarokne/git/BigData/geotweets.tsv", 0.0001)
    print findLocalTime(rdd)
    #saveAsTextFile("/Users/kaisarokne/git/BigData/result_4.tsv", rdd, df)

mainTask4()
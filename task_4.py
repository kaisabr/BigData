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
    from collections import Counter
    localTimeInSeconds = rdd.map(lambda x: (x[1], dt.fromtimestamp((float(x[0]) / 1000) + int(x[8]))))
    lts = sorted(localTimeInSeconds.map(lambda x:(x[0], x[1].hour)).collect())
    localTimeInHours =localTimeInSeconds.map(lambda x:( x[0], x[1].hour))
    test1 = localTimeInSeconds.map(lambda x: (x[0], x[1].hour)).groupByKey().mapValues(lambda x: Counter(x)).collect()

    test = localTimeInHours.groupByKey().mapValues(lambda x: Counter(x)).collect()

    #hoursTest = localTimeInHours.map(lambda x: x[1]).map(Counter).reduceByKey(lambda x,y: x+y)
    #ht = hoursTest.collect()
        #reduceByKey(lambda x: ()).collect()
    print lts
    print test1
    #create dictionary dict = {(countryName, hour): tweetCount}

    '''
    #creates a dictionary of the hour intervals:
    hourDict = (Counter(localTimeInHours[0]))
    print hourDict
    #finds the key in hourDict with the maximum value:
    maxValue = hourDict.most_common(1) # gives (key:value)
    print maxValue
    return maxValue'''

def mainTask4():
    rdd = createRDD("/Users/kaisarokne/git/BigData/geotweets.tsv", 0.00001)
    print findLocalTime(rdd)
    #saveAsTextFile("/Users/kaisarokne/git/BigData/result_4.tsv", rdd, df)

mainTask4()
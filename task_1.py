from __future__ import print_function
from pyspark import SparkContext, SparkConf
import tsv
from operator import add


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

#Counts number of tweets
def a_count(rdd):
    return rdd.count()

#Finds number of distinct users
def b_distinctUsers(rdd):
    un = rdd.map(lambda x: x[7]).distinct().collect()
    print(un[0:10])
    #un = rdd.map(lambda x: x[7]).reduce(count)
    #un2 = un.reduce(count())
    return len(un)

def c_distinctCountries(rdd):
    print()

def d_distinctPlaces(rdd):
    print()

def e_distinctLanguages(rdd):
    print()

def f_minLatitude(rdd):
    print()

def g_minLongitude(rdd):
    print()

def h_maxLatitude(rdd):
    print()

def i_maxLongitude(rdd):
    print()

def j_averageTweetLengthChar(rdd):
    print()

def k_averageTweetLengthWords(rdd):
    print()

def writeToTSV(filename, rdd):
        writer = tsv.TsvWriter(open(filename, "w"))
        #writer.comment("TSV task 1")
        #Task 1a
        #count_tweets = a_count(rdd)
        #writer.line(count_tweets)

        #Task 1b
        count_username = b_distinctUsers(rdd)
        writer.line(count_username)
        #writer.line(c_distinctCountries(rdd))
        #writer.line(d_distinctPlaces(rdd))
        #writer.line(e_distinctLanguages(rdd))
        #writer.line(f_minLatitude(rdd))
        #writer.line(g_minLongitude(rdd))
        #writer.line(h_maxLatitude(rdd))
        #writer.line(i_maxLongitude(rdd))
        #writer.line(j_averageTweetLengthChar(rdd))
        #writer.line(k_averageTweetLengthWords(rdd))
        writer.close()

def mainTask1():
    rdd = createRDD("/Users/vilde/BigData/data/geotweets.tsv", 0.1)
    writeToTSV("/Users/vilde/BigData/result_1.tsv", rdd)

mainTask1()

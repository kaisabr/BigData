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

#Counts distinct users
def b_distinctUsers(rdd):
    usernames = rdd.map(lambda x: x[7]).distinct().collect()
    return len(usernames)

#Counts distinct countries
def c_distinctCountries(rdd):
    countries = rdd.map(lambda x: x[1]).distinct().collect()
    return len(countries)

#Counts distinct places
def d_distinctPlaces(rdd):
    places = rdd.map(lambda x: x[4]).distinct().collect()
    return len(places)

#Counts distinct languages
def e_distinctLanguages(rdd):
    languages = rdd.map(lambda x: x[5]).distinct().collect()
    return len(languages)

#Maa sjekke her om vi skal ha naermest 0 eller mest negativt
def f_minLatitude(rdd):
    latitude = rdd.map(lambda x: x[11]).min()
    return latitude

def g_minLongitude(rdd):
    longitude = rdd.map(lambda x: x[12]).min()
    return longitude

def h_maxLatitude(rdd):
    latitude = rdd.map(lambda x: x[11]).max()
    return latitude

def i_maxLongitude(rdd):
    longitude = rdd.map(lambda x: x[12]).max()
    return longitude


def j_averageTweetLengthChar(rdd):
    average = rdd.map(lambda x: len(x[10])).mean()
    return average

def k_averageTweetLengthWords(rdd):
    average = rdd.map(lambda x: len(x[10].split(" "))).mean()
    return average

def saveAsTextFile(filename, rdd):
        writer = tsv.TsvWriter(open(filename, "w"))

        #Task 1a
        #countTweets = a_count(rdd)
        #writer.line(countTweets)

        #Task 1b
        #countUsername = b_distinctUsers(rdd)
        #writer.line(countUsername)

        #Task 1c
        #countCountries = c_distinctCountries(rdd)
        #writer.line(countCountries)

        #Task 1d
        #countPlaces = d_distinctPlaces(rdd)
        #writer.line(countPlaces)

        #Task 1e
        #countLanguages = e_distinctLanguages(rdd)
        #writer.line(countLanguages)

        #Task 1f
        #minLatitude = f_minLatitude(rdd)
        #writer.line(minLatitude)

        #Task 1g
        #minLongitude = g_minLongitude(rdd)
        #writer.line(minLongitude)

        #Task 1h
        #maxLatitude = h_maxLatitude(rdd)
        #writer.line(maxLatitude)

        #Task 1i
        #maxLongitude = i_maxLongitude(rdd)
        #writer.line(maxLongitude)

        #Task 1j
        #averageTweet = j_averageTweetLengthChar(rdd)
        #writer.line(averageTweet)

        #Task 1k
        #averageWords = k_averageTweetLengthWords(rdd)
        #writer.line(averageWords)

        writer.close()

def mainTask1():
    rdd = createRDD("/Users/vilde/BigData/data/geotweets.tsv", 0.1)
    saveAsTextFile("/Users/vilde/BigData/result_1.tsv", rdd)

mainTask1()

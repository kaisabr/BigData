from pyspark import SparkContext, SparkConf

rdd = None
rdd_sample = None

conf = SparkConf()
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
    usernames = rdd.map(lambda x: x[7]).distinct().count()
    return usernames

#Counts distinct countries
def c_distinctCountries(rdd):
    countries = rdd.map(lambda x: x[1]).distinct().count()
    return countries

#Counts distinct places
def d_distinctPlaces(rdd):
    places = rdd.map(lambda x: x[4]).distinct().count()
    return places

#Counts distinct languages
def e_distinctLanguages(rdd):
    languages = rdd.map(lambda x: x[5]).distinct().count()
    return languages

#Finds minimum latitude
def f_minLatitude(rdd):
    latitude = rdd.map(lambda x: float(x[11])).min()
    return latitude

#Finds minimum longitude
def g_minLongitude(rdd):
    longitude = rdd.map(lambda x: float(x[12])).min()
    return longitude

#Finds maximum latitude
def h_maxLatitude(rdd):
    latitude = rdd.map(lambda x: float(x[11])).max()
    return latitude

#Finds maximum longitude
def i_maxLongitude(rdd):
    longitude = rdd.map(lambda x: float(x[12])).max()
    return longitude

#Finds average tweet length in characters
def j_averageTweetLengthChar(rdd):
    average = rdd.map(lambda x: len(x[10])).mean()
    return average

#Finds average tweet length in words
def k_averageTweetLengthWords(rdd):
    average = rdd.map(lambda x: len(x[10].split(" "))).mean()
    return average

#Runs all parts of the task and saves as tsv file
def run(filename, rdd):
    countTweets = a_count(rdd)
    countUsername = b_distinctUsers(rdd)
    countCountries = c_distinctCountries(rdd)
    countPlaces = d_distinctPlaces(rdd)
    countLanguages = e_distinctLanguages(rdd)
    minLatitude = f_minLatitude(rdd)
    minLongitude = g_minLongitude(rdd)
    maxLatitude = h_maxLatitude(rdd)
    maxLongitude = i_maxLongitude(rdd)
    averageTweet = j_averageTweetLengthChar(rdd)
    averageWords = k_averageTweetLengthWords(rdd)

    #Create a rdd before writing to file
    results = sc.parallelize([countTweets, countUsername, countCountries, countPlaces\
    ,countLanguages, minLatitude, minLongitude, maxLatitude, maxLongitude\
    ,averageTweet, averageWords])

    results.coalesce(1).saveAsTextFile(filename)


def main():
    rdd = createRDD("./data/geotweets.tsv", 1)
    run("./result_1.tsv", rdd)

main()

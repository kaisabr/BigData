from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row

rdd = None
rdd_sample = None
conf = SparkConf()
sc = SparkContext(conf = conf)
context = SQLContext(sc)
sc.setLogLevel("OFF")

#Initializes RDD
def createRDD(filename, val):
    rdd = sc.textFile(filename).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample

 # change name and type of columns:
def setRow(rdd):
    row = rdd.map(lambda x: Row(
        utc_time = float(x[0]),
        country_name = x[1],
        country_code = x[2],
        place_type = x[3],
        place_name = x[4],
        language_ = x[5],
        username = x[6],
        user_screen_name = x[7],
        timezome_offset = float(x[8]),
        number_of_friends = int(x[9]),
        tweet_text = x[10],
        latitude = float(x[11]),
        longitude = float(x[12])
    ))
    return row

#create data table from rdd
def createTable(row):
    df = context.createDataFrame(row)
    table = df.registerTempTable("tweets")
    return table

#task 8a: Count the number of tweets:
def a_count():
    noTweets = context.sql("SELECT count(*) AS number_of_tweets FROM tweets")
    noTweets.show()

#task 8b: Number of distinct users (by selecting on user_screen_name):
def b_distinctUsers():
    distUsers = context.sql("SELECT count(DISTINCT user_screen_name) AS distinct_users FROM tweets")
    distUsers.show()

#task 8c: Number of distinct countries:
def c_distinctCountries():
    distCountries = context.sql("SELECT count(DISTINCT country_name) AS distinct_countries FROM tweets")
    distCountries.show()

#task 8d: Number of distinct places:
def d_distinctPlaces():
    distPlaces = context.sql("SELECT count(DISTINCT place_name) AS distinct_places FROM tweets")
    distPlaces.show()

#task 8e: Number of distinct languages tweets are posted in:
def e_distinctLanguages():
    distLanguages = context.sql("SELECT count(DISTINCT language_) AS distinct_language FROM tweets")
    distLanguages.show()

#task 8f: Minimum value of latitude:
def f_minLatitude():
    minLat = context.sql("SELECT MIN(latitude) AS minimum_latitude FROM tweets")
    minLat.show()

#task 8f: Minimum value of longtitude:
def f_minLongitude():
    minLon = context.sql("SELECT MIN(longitude) AS minimum_longitude FROM tweets")
    minLon.show()

#task 8g: Maximum value of latitude:
def g_maxLatitude():
    maxLat = context.sql("SELECT MAX(latitude) AS maximum_latitude FROM tweets")
    maxLat.show()

#task 8g: Maxmumim value of longitude:
def g_maxLongitude():
   maxLon = context.sql("SELECT MAX(longitude) AS maximum_longitude FROM tweets")
   maxLon.show()

#main function, runs program
def mainTask8():
    rdd = createRDD("/Users/kaisarokne/git/BigData/geotweets.tsv", 1)
    row = setRow(rdd)
    table = createTable(row)
    a_count()
    b_distinctUsers()
    c_distinctCountries()
    d_distinctPlaces()
    e_distinctLanguages()
    f_minLatitude()
    f_minLongitude()
    g_maxLatitude()
    g_maxLongitude()

mainTask8()

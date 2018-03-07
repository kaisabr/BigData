from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession



rdd = None
rdd_sample = None
conf = SparkConf()
sc = SparkContext(conf = conf)

#Create SparkSession:
spark = SparkSession \
    .builder \
    .config(conf = SparkConf()) \
    .getOrCreate()

#Initializes RDD
def createRDD(filename, val):
    rdd = sc.textFile(filename).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample

# change name of columns:
'''
utc_time = 0
country_name = 1
country_code = 2
place_type = 3
place_name = 4
language = 5
username = 6
user_screen_name = 7
timezome_offset = 8
number_of_friends = 9
tweet_text = 10
latitude = 11
longitude = 12
'''
#create data frame df from rdd
def createDF(rdd):
    df = spark.createDataFrame(rdd, ("utc_time", "country_name", "country_code", \
                                     "place_type", "place_name", "language", \
                                     "username", "user_screen_name", "timezome_offset", \
                                     "number_of_friends", "tweet_text", "latitude", "longitude"))
    return df

#task 8a: Count the number of tweets:
def a_count(df):
    return df.count()

#task 8b: Number of distinct users (username):
def b_distinctUsers(df):
    distUsers = df.select("user_screen_name").distinct().count()
    return distUsers

#task 8c: Number of distinct countries:
def c_distinctCountries(df):
    distCountries = df.select("country_name").distinct().count()
    return distCountries

#task 8d: Number of distinct places:
def d_distinctPlaces(df):
    distPlaces = df.select("place_name").distinct().count()
    return distPlaces

#task 8e: Number of distinct languages tweets are posted in:
def e_distinctLanguages(df):
    distLanguages = df.select("language").distinct().count()
    return distLanguages

#task 8f: Minimum value of latitude:
def f_minLatitude(df):
    df_num = df.select(df.latitude.cast("float"))
    minLat = df_num.groupBy().min('latitude').collect()[0]
    return minLat["min(latitude)"]

#task 8f: Minimum value of longtitude:
def f_minLongitude(df):
    df_num = df.select(df.longitude.cast("float"))
    minLong = df_num.groupBy().min('longitude').collect()[0]
    return minLong["min(longitude)"]

#task 8g: Maximum value of latitude:
def g_maxLatitude(df):
    df_num = df.select(df.latitude.cast("float"))
    maxLat = df_num.groupBy().max('latitude').collect()[0]
    return maxLat["max(latitude)"]

#task 8g: Maxmumim value of longitude:
def g_maxLongitude(df):
    df_num = df.select(df.longitude.cast("float"))
    maxLong = df_num.groupBy().max('longitude').collect()[0]
    return maxLong["max(longitude)"]

#main function, runs program
def mainTask4():
    rdd = createRDD("/Users/kaisarokne/git/BigData/geotweets.tsv", 0.1)
    df = createDF(rdd)

    print("Tweet count: ", a_count(df), \
          "Distinct users: ", b_distinctUsers(df), \
          "Distinct countries: ", c_distinctCountries(df), \
          "Distinct places: ",d_distinctPlaces(df), \
          "Distinct languages: ", e_distinctLanguages(df), \
          "Minimum longitude: ", f_minLongitude(df), \
          "Maximum longitude: ", g_maxLongitude(df), \
          "Minimum latitude: ", f_minLatitude(df), \
          "Maximum latitude:", g_maxLatitude(df))

mainTask4()

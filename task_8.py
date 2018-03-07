from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import tsv


rdd = None
rdd_sample = None
conf = SparkConf()
#conf.setMaster("local")
#conf.setAppName("My application")
#conf.set("spark.executor.memory", "1g")
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
    distUsers = df.select("username").distinct().count()
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
    distLanguages = df.select("languages").distinct().count()
    return distLanguages

#task 8f: Minimum value of latitude:
def f_minLatitude(df):
    df.latitude = df.latitude.astype(float)
    minLat = df.latitude.min()
    return minLat

#task 8f: Minimum value of longtitude:
def f_minLongitude(df):
    minLong = df.select("longitude").min()
    return minLong

#task 8g: Maximum value of latitude:
def g_maxLatitude(df):
    #df['MyColumnName'] = df['MyColumnName'].astype('float64')
    maxLatRow = df.agg({"latitude": "max"}).collect()[0]
    maxLat = maxLatRow["max(latitude)"]
    #maxLat = df.select("latitude").rdd.max()
    return maxLat

#task 8g: Maxmumim value of longitude:
#  def g_maxLongitude(df):
    # df.registerTempTable("df_table")
# df.groupby().max("longitude").collect()[0].asDict()['max(longitude)']    #maxLong = df.select(max("longitude"))
    #pyspark.sql.utils.AnalysisException: u'"longitude" is not a numeric column. Aggregation function can only be applied on a numeric column.
    #return maxLong


def mainTask4():
    rdd = createRDD("/Users/kaisarokne/git/BigData/geotweets.tsv", 0.1)
    df = createDF(rdd)
    df.show(2)
    print('HALLOOOOOOO ', f_minLatitude(df))
    #print("HEEEEI, ", g_maxLongitude(df))
    #a_count(df)
    #b_distinctUsers(df)
    #g_maxLatitude(df)
    ("number of tweets: ", a_count(df))
    #print("distinct number of users: ", b_distinctUsers(df))
    #print(c_distinctCountries(df))
    #print(d_distinctPlaces(df))
    #c_distinctCountries(df)
    #saveAsTextFile("/Users/kaisarokne/git/BigData/result_4.tsv", rdd, df)


mainTask4()

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

#create DF
def createDF(rdd):
    df = spark.createDataFrame(rdd, ("utc_time", "country_name", "country_code", \
                                     "place_type", "place_name", "language", \
                                     "username", "user_screen_name", "timezome_offset", \
                                     "number_of_friends", "tweet_text", "latitude", "longitude"))
    return df

#create data frame df from rdd, change name of columns

#task 4a: Count the number of tweets:
def a_count(df):
    return df.count()
#df2 = spark.sql("SELECT field1 AS f1, field2 as f2 from table1")

#task 4b: Number of distinct users (username):
def b_distinctUsers(df):
    distUsers = df.select("username").distinct().count()
    return distUsers
#task 4c: Number of distinct countries:
def c_distinctCountries(df):
    distCountries = df.select("country_name").distinct().count()
    return distCountries

#task 4d: Number of distinct places:
def d_distinctPlaces(df):
    distPlaces = df.select("place_name").distinct().count()
    return distPlaces

#task 4e: Number of distinct languages tweets are posted in:
def e_distinctLanguages(df):
    distLanguages = df.select("languages").distinct().count()
    return distLanguages

#task 4f:

#task 4g


def saveAsTextFile(filename, rdd, df):
        writer = tsv.TsvWriter(open(filename, "w"))
        #write 4c to result_4.tsv
        #writer.line(c_distinctCountries(df))
        writer.close()

def mainTask4():
    rdd = createRDD("/Users/kaisarokne/git/BigData/geotweets.tsv", 0.1)
    df = createDF(rdd)

    #print("number of tweets: ", a_count(df))
    #print("distinct number of users: ", b_distinctUsers(df))
    #print(c_distinctCountries(df))
    print(d_distinctPlaces(df))
    #c_distinctCountries(df)
    saveAsTextFile("/Users/kaisarokne/git/BigData/result_4.tsv", rdd, df)


mainTask4()

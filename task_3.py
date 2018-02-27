from pyspark import SparkContext, SparkConf
import tsv


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

def countTweets(rdd):
    countries = rdd.map(lambda x: x[1]).countByValue().items()#filter(lambda (value,count): count>= 10))
    #print(countries)
    countries10 = map(lambda x: x[0],list(filter(lambda (v,c): c >= 10, countries)))
    #print("More than 10: ", countries10)
    #print("Less than 10: ", countries)
    return countries10

def filterRDD(rdd):
    countries = countTweets(rdd)
    rdd2 = rdd.filter(lambda x: x[1] in countries).
    #print("Count: ", rdd2.count())
    return rdd2


def saveAsTextFile(filename, rdd):
        writer = tsv.TsvWriter(open(filename, "w"))
        #Må skrives ferdig for å skrive land, latitude, longitude
        for element in rdd:
            writer.line()
        writer.close()

def mainTask3():
    rdd = createRDD("/Users/vilde/BigData/data/geotweets.tsv", 0.1)
    filterRDD(rdd)
    saveAsTextFile("/Users/vilde/BigData/result_3.tsv", rdd)

mainTask3()

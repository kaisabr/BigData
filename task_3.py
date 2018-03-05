from pyspark import SparkContext, SparkConf
import tsv


rdd = None
rdd_sample = None

conf = SparkConf()
sc = SparkContext(conf = conf)

#Initializes RDD
def createRDD(filename, val):
    rdd = sc.textFile(filename).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample

def countTweets(rdd):
    countries = rdd.map(lambda x: x[1]).countByValue().items()#filter(lambda (value,count): count>= 10))
    countries10 = map(lambda x: x[0],list(filter(lambda (v,c): c >= 10, countries)))
    return countries10

def filterRDD(rdd, countries):
    rdd10 = rdd.filter(lambda x: x[1] in countries)
    return rdd10

def calculateCentroid(rdd, country):
    latitude = rdd.filter(lambda y: y[1]==country).map(lambda x: float(x[11])).mean()
    longitude = rdd.filter(lambda y: y[1]==country).map(lambda x: float(x[12])).mean()
    return latitude, longitude

def saveFile(filename, rdd):
        #writer = tsv.TsvWriter(open(filename, "w"))
        countries = countTweets(rdd)
        rddFiltered = filterRDD(rdd, countries)
        #Maa skrives ferdig for aa skrive land, latitude, longitude
        result = []

        for country in countries:
            latitude, longitude = calculateCentroid(rddFiltered, country)
            result.append((country+"\t"+str(latitude)+"\t"+str(longitude)))
        #    writer.line(country+"\t"+str(latitude)+"\t"+str(longitude))
        #writer.close()

        resultRdd = result.parallelize(result)
        resultRdd.coalesce(1).saveAsTextFile(filename)

def mainTask3():
    rdd = createRDD("/Users/vilde/BigData/data/geotweets.tsv", 0.001)
    saveFile("/Users/vilde/BigData/result_3", rdd)

mainTask3()

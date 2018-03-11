from pyspark import SparkContext, SparkConf
import tsv


rdd = None
rdd_sample = None

conf = SparkConf()
sc = SparkContext(conf = conf)

# Initializes RDD - splits on tab.
# Val is the percentage to read from file. Default to 0.1.
def createRDD(filename, val):
    rdd = sc.textFile(filename, use_unicode=True).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample

# Counts tweets per country by countryname as key
def countTweetsCountry(rdd):
    countries = rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x+y)\
    .filter(lambda x: int(x[1])>=10)\
    .map(lambda x: x[0]).collect()
    return countries

# Filters rdd so that it removes all countries with less than 10 tweets
def filterRDD(rdd, countries):
    rddFiltered = rdd.filter(lambda x: x[1] in countries)
    return rddFiltered

# Calculates centroid with average latitude and longitude
def calculateCentroid(rdd):
    latCount = (0,0)
    latitude = rdd.map(lambda x: (x[1], float(x[11])))\
        .aggregateByKey(latCount, lambda x,y: (x[0] + y, x[1] + 1), lambda x,y: (x[0] + y[0], x[1] + y[1]))\
        .mapValues(lambda a: a[0]/a[1])

    longCount = (0,0)
    longitude = rdd.map(lambda x: (x[1], float(x[12])))\
        .aggregateByKey(longCount, lambda x,y: (x[0] + y, x[1] + 1), lambda x,y: (x[0] + y[0], x[1] + y[1]))\
        .mapValues(lambda a: a[0]/a[1])

    return latitude.join(longitude).map(lambda x: str(x[0]) + "\t" + str(x[1][0]) + "\t" + str(x[1][1]))


def main():
    rdd = createRDD("./data/geotweets.tsv", 1)
    countries = countTweetsCountry(rdd)
    rddFiltered = filterRDD(rdd, countries)
    rddWrite = calculateCentroid(rddFiltered)
    rddWrite.coalesce(1).saveAsTextFile("./result_3.tsv")


main()

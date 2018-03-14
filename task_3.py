from pyspark import SparkContext, SparkConf


#Initializes spark config and context
rdd = None
rdd_sample = None

conf = SparkConf()
sc = SparkContext(conf = conf)

#Initializes RDD - loading the dataset into an RDD using textFile
#Maps each line in the dataset so that it splits on tab
#Samples "val" percentage of that RDD (e.g. val = 0.1 --> 10% of dataset)
def createRDD(filename, val):
    rdd = sc.textFile(filename, use_unicode=True).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample

#Sets country name (using column 1 countr_name) as key and starting the count at 1
#Use reduceByKey to count up number of tweets per country
#Filters out all countries not having more than 10 tweets
#Maps so that we're left with countries - return a list of countries
def countTweetsCountry(rdd):
    countries = rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x+y)\
    .filter(lambda x: int(x[1])>=10)\
    .map(lambda x: x[0]).collect()
    return countries

#Filters RDD so that it removes all countries with less than 10 tweets
def filterRDD(rdd, countries):
    rddFiltered = rdd.filter(lambda x: x[1] in countries)
    return rddFiltered

#Calculates centroid with average latitude and longitude
#Uses country_name (column 1) and latitude/longitude (column 11/12)
#Aggregates by key using aggregateByKey and sums up latitude/longitude for each country
#The tuple latCount sums up latitudes and longCount sums up longitudes
#Lastly we map values using mapValues where we divide the sum of latitudes/longitudes with total number of one country
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
    rddCentroids = calculateCentroid(rddFiltered)
    rddCentroids.coalesce(1).saveAsTextFile("./result_3.tsv")


main()

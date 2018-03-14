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

#Filters out all tweets not with country_code US (column 2) and not place_type city (column 3)
#Maps so that we're left with column 4 - place_name
#Counts number of tweets by value (place name) and sorts first by count, then by country in alphabetical order.
#Creates RDD with city name <tab> count using parallelize.
def countCityUS(rdd):
    rddFiltered = rdd.filter(lambda x: x[2]=='US' and x[3] =='city').map(lambda y: y[4])
    rddCount = rddFiltered.countByValue().items()
    listSorted = sorted(rddCount, key = lambda x: (x[1]*(-1), x[0]))
    rddSorted = sc.parallelize(listSorted).map(lambda (x,y): str(x) + "\t" + str(y))
    return rddSorted


def main():
    rdd = createRDD("./data/geotweets.tsv", 1)
    rddSorted = countCityUS(rdd)
    rddSorted.coalesce(1).saveAsTextFile("./result_5.tsv")


main()

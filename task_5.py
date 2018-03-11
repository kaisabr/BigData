from pyspark import SparkContext, SparkConf
import tsv

#Initializes SparkContext and SparkConf
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

# Filters out all tweets not with country_code US and not place_type city
# Counts number of tweets and sorts first by count, then by country in alphabetical order.
# Creates rdd with city name <tab> count.
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

from pyspark import SparkContext, SparkConf
import tsv


rdd = None
rdd_sample = None

conf = SparkConf()
sc = SparkContext(conf = conf)

# Initializes RDD - splits on tab.
# Val is the percentage to read from file. Default to 0.1.
def createRDD(filename, val):
    rdd = sc.textFile(filename).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample

# Counts tweets by country and sorts first by count, then country.
# Returns sorted list
def sortCount(rdd):
    rddCount = rdd.map(lambda x: x[1]).countByValue().items()
    rddSorted = sorted(rddCount, key = lambda x: (x[1]*(-1), x[0]))
    return rddSorted

# Function that saves to tsv file
def saveAsTextFile(filename, rdd):
    writer = tsv.TsvWriter(open(filename, "w"))
    for country, count in rdd:
        writer.line(country + "\t" + str(count))
    writer.close()

def mainTask2():
    rdd = createRDD("/Users/vilde/BigData/data/geotweets.tsv", 0.1)
    rddSorted = sortCount(rdd)
    saveAsTextFile("/Users/vilde/BigData/result_2.tsv", rddSorted)

mainTask2()

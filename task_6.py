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

def frequentWords(rdd):
    rddFiltered = rdd.filter(lambda x: x[2] == 'US')
    rddMapped = rddFiltered.map(lambda x: x[10].lower().split(" "))
    return rddMapped


def mainTask6():
    rdd = createRDD("./data/geotweets.tsv", 0.1)
    freqWords = frequentWords(rdd)

mainTask6()

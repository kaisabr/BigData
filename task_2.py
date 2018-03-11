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

# Counts tweets by country and sorts first by count, then country.
# Returns sorted list
def sortCount(rdd):
    rddCount = rdd.map(lambda x: x[1]).countByValue().items()
    rddSorted = sorted(rddCount, key = lambda x: (x[1]*(-1), x[0]))
    return rddSorted

#Sorting the list and creating RDD before writing to file formated as tsv
def run(rdd):
    listSorted = sortCount(rdd)
    rddSorted = sc.parallelize(listSorted).map(lambda (x,y): x + "\t" + str(y))
    return rddSorted

#Creates rdd before writing to file
def main():
    rdd = run(createRDD("./data/geotweets.tsv", 1))
    rdd.coalesce(1).saveAsTextFile("./result_2.tsv")

main()

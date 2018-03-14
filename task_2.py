from pyspark import SparkContext, SparkConf
import tsv

#Initializes SparkContext and SparkConf
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

#Counts tweets by country (using column 1) with function countByValue where it counts by the country names
#Collects the counted values into a list
#Sorts list first by count in descending order, then by country name alphabetically
#Returns sorted list
def sortCount(rdd):
    rddCount = rdd.map(lambda x: x[1]).countByValue().items()
    rddSorted = sorted(rddCount, key = lambda x: (x[1]*(-1), x[0]))
    return rddSorted

#Sorting the list and creating an RDD using parallelize
#Returns sorted RDD
def run(rdd):
    listSorted = sortCount(rdd)
    rddSorted = sc.parallelize(listSorted).map(lambda (x,y): x + "\t" + str(y))
    return rddSorted

#Runs the functions before writing to file using saveAsTextFile
def main():
    rdd = run(createRDD("./data/geotweets.tsv", 1))
    rdd.coalesce(1).saveAsTextFile("./result_2.tsv")

main()

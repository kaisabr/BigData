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

def filterCityUS(rdd):
    rddFiltered = rdd.filter(lambda x: x[2]=='US' and x[3] =='city').map(lambda y: y[4])
    rddCount = rddFiltered.countByValue().items()
    rddSorted = sorted(rddCount, key = lambda x: (x[1]*(-1), x[0]))
    return rddSorted

def saveAsTextFile(filename, rdd):
        writer = tsv.TsvWriter(open(filename, "w"))
        for country, count in rdd:
            writer.line(country + "\t" + str(count))
        writer.close()

def mainTask5():
    rdd = createRDD("/Users/vilde/BigData/data/geotweets.tsv", 0.1)
    rddSorted = filterCityUS(rdd)
    saveAsTextFile("/Users/vilde/BigData/result_5.tsv", rddSorted)

mainTask5()

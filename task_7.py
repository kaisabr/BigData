from pyspark import SparkContext, SparkConf

rdd = None
rdd_sample = None

conf = SparkConf()
sc = SparkContext(conf = conf)

#Initializes RDD
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
    rddSorted = sc.parallelize(listSorted).take(5)
    return rddSorted

#Reads stopWords from file, and returns a list of all stopwords.
def stopWordsList(filename):
    stopWords = []
    wordList = open(filename, 'r')
    for word in wordList:
        stopWords.append(word.strip())
    wordList.close()
    return stopWords

#Filters out words shorter than 2 characters and words in stopWords
#Maps each row so that it starts counting at 1, then counts up all words
#Sorts list by count, descending
def frequentWords(rdd, stopWords):
    rddFrequent = rdd.filter(lambda x: not(len(x)<2)).filter(lambda x: x not in stopWords)\
        .map(lambda y: (y, 1)).reduceByKey(lambda x, y: x + y)\
        .sortBy(lambda x: x[1], ascending = False)
    return rddFrequent


def mainTask7():
    rdd = createRDD("./data/geotweets.tsv", 0.1)

    #saveAsTextFile("./result_7.tsv", rdd)

mainTask7()

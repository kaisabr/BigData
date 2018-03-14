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

#Filters out tweets not coming from the US using column 2 (country_code)
#Then split each tweet into list of words using flatMap where we extract column 10 - tweet_text
def mapUSTweets(rdd):
    rddWords = rdd.filter(lambda x: x[2] == 'US').flatMap(lambda x: x[10].lower().split(" "))
    return rddWords

#Reads stopWords from file, and returns a list of all stopwords.
def stopWordsList(filename):
    stopWords = []
    wordList = open(filename, 'r')
    for word in wordList:
        stopWords.append(word.strip())
    wordList.close()
    return stopWords

#Filters out words shorter than 2 characters and words in stopWords using filter function.
#Maps each row so that it starts counting at 1, then counts up all words using reduceByKey where key is words
#Sorts list by count in descending manner
def frequentWords(rdd, stopWords):
    rddFrequent = rdd.filter(lambda x: not(len(x)<2)).filter(lambda x: x not in stopWords)\
        .map(lambda y: (y, 1)).reduceByKey(lambda x, y: x + y)\
        .sortBy(lambda x: x[1], ascending = False)
    return rddFrequent

#Creates RDD and reads stopwords list from file
#Counts words as done in frequentWords()
#Finds 10 most frequent words and writes to file using saveAsTextFile
def main():
    rdd = createRDD("./data/geotweets.tsv", 1)
    stopWords = stopWordsList("./data/stop_words.txt")
    rddWords = mapUSTweets(rdd)
    mostFreqWords = frequentWords(rddWords, stopWords).take(10)
    results = sc.parallelize(mostFreqWords).map(lambda x: x[0] + "\t" + str(x[1]))
    results.coalesce(1).saveAsTextFile("./result_6.tsv")


main()

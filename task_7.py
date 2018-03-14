from pyspark import SparkContext, SparkConf


conf = SparkConf()
sc = SparkContext(conf = conf)

#Initializes RDD - loading the dataset into an RDD using textFile
#Maps each line in the dataset so that it splits on tab
#Samples "val" percentage of that RDD (e.g. val = 0.1 --> 10% of dataset)
def createRDD(filename, val):
    rdd = sc.textFile(filename, use_unicode=True).map(lambda line: line.split('\t'))
    rdd_sample = rdd.sample(False, val, 5)
    return rdd_sample

#Filters out all tweets not with country_code=US or place_type=city before mapping to place_name
#Counts all tweets from a given city using map and reduceByKey where the key is city_name
#Then sorts by key (alphabetically) then by count in descending order. Take the 5 top cities.
def countCityUS(rdd):
    rddCount = rdd.filter(lambda x: x[2]=='US' and x[3] =='city')\
            .map(lambda x: ((x[4], 1))).reduceByKey(lambda x,y: x+y)
    topCities = rddCount.sortByKey().sortBy(lambda x: x[1], ascending = False).map(lambda x: x[0]).take(5)
    return topCities

#Reads stopWords from file, and returns a list of all stopwords.
def stopWordsList(filename):
    stopWords = []
    wordList = open(filename, 'r')
    for word in wordList:
        stopWords.append(word.strip())
    wordList.close()
    return stopWords

def filterRDD(rdd, cities):
    rddTopCities = rdd.filter(lambda x: x[4] in cities)
    return rddTopCities

def countWords(rdd, stopWords):
    #Flatmaps column 4 (place_name) so that it makes a tuple (place_name, words)
    words = rdd.flatMap(lambda x: ((x[4], w) for w in x[10].lower().split(" ")))

    #Filters out words with length less than 2 and words in stopwords
    filteredWords = words.filter(lambda x: not(len(x[1])<2))\
            .filter(lambda x: x[1] not in stopWords)\
            .map(lambda x: ((x[0], x[1]), 1))

    #Counts number of words for a given key, here words per city
    countRDD = filteredWords.reduceByKey(lambda x,y: x+y)\
            .map(lambda x: ((x[0][0]), (x[0][1], x[1])))

    #Sorts alpha then by count.
    #Groups by key and casting the words with the respective counts to a list.
    #Maps so that we're left with city_name and the respective 10 most popular words
    topWords = countRDD.sortBy(lambda x: x[1][0], ascending=True).sortBy(lambda x: x[1][1], ascending=False)\
            .groupByKey().map(lambda x: (x[0], list(x[1])))\
            .map(lambda x: (x[0], x[1][0:10]))

    return topWords

def writeRDD(rdd, filename):
    rdd.map(lambda x: x[0] + "\t" + str(x[1][0][0]) + "\t" + str(x[1][0][1])\
     + "\t" + str(x[1][1][0]) + "\t" + str(x[1][1][1]) + "\t" + str(x[1][2][0])\
     + "\t" + str(x[1][2][1]) + "\t" + str(x[1][3][0]) + "\t" + str(x[1][3][1])\
     + "\t" + str(x[1][4][0]) + "\t" + str(x[1][4][1]) + "\t" + str(x[1][5][0])\
     + "\t" + str(x[1][5][1]) + "\t" + str(x[1][6][0]) + "\t" + str(x[1][6][1])\
     + "\t" + str(x[1][7][0]) + "\t" + str(x[1][7][1]) + "\t" + str(x[1][8][0])\
     + "\t" + str(x[1][8][1]) + "\t" + str(Fx[1][9][0]) + "\t" + str(x[1][9][1]))\
        .coalesce(1).saveAsTextFile(filename)


def main():
    stopWords = stopWordsList("./data/stop_words.txt")

    rdd = createRDD("./data/geotweets.tsv", 1)
    topCities = countCityUS(rdd)

    rddFiltered = filterRDD(rdd, topCities)
    wordCount = countWords(rddFiltered, stopWords)

    writeRDD(wordCount, "./result_7.tsv")


main()

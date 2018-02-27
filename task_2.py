from pyspark import SparkContext, SparkConf

conf = SparkConf()
#conf.setMaster("local")
#conf.setAppName("My application")
#conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf = conf)

data_file = "/Users/vilde/BigData/data/geotweets.tsv"
rdd = sc.textFile(data_file)

rdd_sample = rdd.sample(False, 0.1, 5)

count = rdd_sample.count()

print("HELOOOOOO", count)

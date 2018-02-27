from pyspark import SparkContext, SparkConf
import tsv

conf = SparkConf()
#conf.setMaster("local")
#conf.setAppName("My application")
#conf.set("spark.executor.memory", "1g")
sc = SparkContext(conf = conf)

#data_file = "/Users/vilde/BigData/data/geotweets.tsv"
#rdd = sc.textFile(data_file)

#rdd_sample = rdd.sample(False, 0.1, 5)

#count = rdd_sample.count()

#print("HELOOOOOO", count)


writer = tsv.TsvWriter(open("task_1.tsv", "w"))

writer.comment("This is a comment")
writer.line("Column 1", "Column 2", 12345)
writer.list_line(["Column 1", "Column 2"] + list(range(10)))
writer.close()

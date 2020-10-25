#setting up spark envi in bridges
#create spark context object, tell spark how to access a cluster
#but first, build a sparkconf object that contains infor about your application
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf=conf)

print('hello world')
# turn into a rdd structure
rdd = sc.textFile("Complete_Shakespeare.txt")
# count number of lines
print(rdd.count())
######### tips: do everything HEAVY with rdd, not with python, for performance purpose
rddmap= rdd.flatMap(lambda line: line.split() ) 
print(rddmap.count())
# count unique words
print(rddmap.distinct().count())
# count recurrence of unique words ( like key - value )
key_value_rdd =rddmap.map(lambda x: (x,1))# make words into tuple. map x to (x,1)
print(key_value_rdd.take(5)) # first 5 elements
# reduce: apply a function on values (here sum) when key are equal (collapse by key)
word_counts_rdd = key_value_rdd.reduceByKey(lambda x,y: x+y)
print(word_counts_rdd)
print(word_counts_rdd.take(5))
#finding the 5 most frequent
flipped_rdd = word_counts_rdd.map(lambda x: (x[1],x[0]))# mapping x to flipped x
print(flipped_rdd.take(5))
results_rdd = flipped_rdd.sortByKey(False)
print(results_rdd.take(5))# first 5 elements




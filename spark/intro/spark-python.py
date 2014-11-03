#!/usr/bin/python

from pyspark import SparkContext

logfile = "/root/war_peace_text"
sc = SparkContext("local", "SparkHelloWorld")
sc.textFile(logfile)
wordcount = logfile.map(lambda s: s.startswith('A')).reduce(lambda a,b: a+b)
print(wordcount)


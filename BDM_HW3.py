import sys
import csv
from pyspark import SparkContext

if __name__=='__main__':
    file = sys.argv[0]
    output = sys.argv[1]
    sc = SparkContext()
    sc.textFile(file) \
    .mapPartitions(lambda partition: csv.reader([line.replace('\n','') for line in partition],delimiter=',', quotechar='"')) \
    .filter(lambda line: len(line) > 7 and line[0] != 'Date received' and len(line[0])==10) \
    .map(lambda line: ((line[1], line[0][:4], line[7]), 1)) \
    .groupByKey() \
    .mapValues(lambda values: sum(values)) \
    .map(lambda line: ((line[0][0], line[0][1]), line[1])) \
    .groupByKey()\
    .mapValues(lambda x: (sum(x), len(x), round(max(x)*100/sum(x)))) \
    .sortByKey() \
    .map(lambda line: (line[0][0], line[0][1], line[1][0], line[1][1], line[1][2])) \
    .saveAsTextFile(output)